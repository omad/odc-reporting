import gzip
import os.path
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

import click
import csv
import dawg
import json
import psycopg2
import re
from itertools import groupby
from pathlib import Path
from psycopg2.extras import NamedTupleCursor
from tqdm import tqdm

from odc.aws import inventory
from odc.aws import s3_fetch, s3_client, s3_download


def load_inventory_manifest(url):
    s3 = s3_client()

    if url.endswith("/"):
        url = inventory.find_latest_manifest(url, s3)

    info = s3_fetch(url, s3=s3)
    return json.loads(info)


def get_inventory_urls(manifest):
    """Get all the URLs for the latest complete CSV inventory"""
    must_have_keys = {"fileFormat", "fileSchema", "files", "destinationBucket"}
    missing_keys = must_have_keys - set(manifest)
    if missing_keys:
        raise ValueError("Manifest file haven't parsed correctly")

    if manifest["fileFormat"].upper() != "CSV":
        raise ValueError("Data is not in CSV format")

    s3_prefix = "s3://" + manifest["destinationBucket"].split(":")[-1] + "/"
    data_urls = [s3_prefix + f["key"] for f in manifest["files"]]
    return data_urls


def _load_inventory_file_list(filename, predicate):
    with gzip.open(filename, mode='rt', encoding='utf8') as fin:
        reader = csv.reader(fin)
        s3_urls = (f"s3://{bucket}/{key}" for bucket, key, _, _ in reader)
        return [url for url in s3_urls in if predicate(url)]


def stream_inventory_parallel(predicate=None, max_workers=None):
    if predicate is None:
        predicate = lambda x: True

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        print(f'Using {executor._max_workers} workers.')
        futures = []
        for csvfile in Path(".").glob('*.csv.gz'):
            futures.append(executor.submit(_load_inventory_file_list, csvfile, predicate))

        for future in tqdm(as_completed(futures), desc='Inventory file', unit='files', total=len(futures)):
            yield from future.result()


def locations_from_inventory(prefix, suffix, inventory_dawg_file):
    all_datasets = dawg.CompletionDAWG()
    all_datasets.load(inventory_dawg_file)

    return dawg.CompletionDAWG(url for url in all_datasets.keys(prefix) if url.endswith(suffix))


def product_locations_from_database(conn, product_name):
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
            select uri_scheme, uri_body
            from agdc.dataset_location dl
            left join agdc.dataset d on dl.dataset_ref = d.id
            left join agdc.dataset_type dt on d.dataset_type_ref = dt.id
            where dt.name = '{product_name}'
            and dl.archived is null
            and d.archived is null
            """)
            return dawg.CompletionDAWG(f"{scheme}:{body}" for scheme, body in cursor)


def _common_paths_for_uris(uri_samples):
    def uri_scheme(uri: str):
        return uri.split(":", 1)[0]

    for scheme, uris in groupby(sorted(uri_samples), uri_scheme):
        uris = list(uris)

        # Use the first, last and middle as examples
        # (they're sorted, so this shows diversity)
        example_uris = {uris[0], uris[-1], uris[int(len(uris) / 2)]}
        #              ⮤ we use a set for when len < 3

        # munge an extra '/' in after the scheme
        scheme, common_path = os.path.commonpath(uris).split(':')

        return [f"{scheme}:/{common_path}", sorted(example_uris)]


def find_product_locations(sample_size=1000):
    all_products = list(index.products.get_all())
    unioned_query = "\nUNION ALL\n".join(f"""
    (select '{dataset_type.name}', dl.uri_scheme || ':' || dl.uri_body
    from dataset_location dl
    left join dataset d on dl.dataset_ref = d.id
    where d.dataset_type_ref = {dataset_type.id}
    and d.archived is null
    limit {sample_size})
    """ for dataset_type in all_products)

    product_urls = defaultdict(list)
    for product_name, uri in conn.execute(unioned_query):
        product_urls[product_name].append(uri)

    return {
        name: _common_paths_for_uris(uris)
        for name, uris in product_urls.items()
    }


def list_all_products(conn: psycopg2.extensions.connection):
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as cursor:
            cursor.execute('select id, name from agdc.dataset_type')
            # is_there_a_match = False if curs.fetchone() is None else True
            all_products = list(cursor.fetchall())

    return all_products


def find_product_locations(conn: psycopg2.extensions.connection, sample_size=10000):
    """
    Query ODC Database and Return a summary of all Products' storage locations

    {
        'product_name': ['s3://common_path', [complete example paths]],
        ...
    }

    Based on code in datacube-explorer.
    """
    all_products = list_all_products(conn)
    unioned_query = "\nUNION ALL\n".join(f"""
    (select '{dataset_type.name}' as name, dl.uri_scheme || ':' || dl.uri_body as url
    from agdc.dataset_location dl
    left join agdc.dataset d on dl.dataset_ref = d.id
    where d.dataset_type_ref = {dataset_type.id}
    and d.archived is null
    limit {sample_size})
    """ for dataset_type in all_products)

    product_urls = defaultdict(list)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(unioned_query)
            for product_name, uri in cursor:
                product_urls[product_name].append(uri)

    return {
        name: _common_paths_for_uris(uris)
        for name, uris in product_urls.items()
    }


def count_by_year(urls):
    extract_year = re.compile(r"(\d\d\d\d)")
    try:
        years = (extract_year.search(url).group() for url in urls)
        return Counter(year for year in years if year is not None)
    except AttributeError:
        return Counter()


def compare_all_products(prod_locations, conn, inventory_dawg_file):
    all_product_report = {}
    for product_name, location_information in tqdm(prod_locations.items(), desc="Processing products", unit="products"):
        prefix, examples = location_information
        print(f'Processing {product_name}')

        # Find the common suffix of these locations, typically either .json or .yaml
        suffix = examples[0].rsplit('.', 1)[1]

        print(f'Prefix: "{prefix}". Suffix "{suffix}"')

        print(f'Loading locations from DB')
        # Load all locations for this product from database
        db_locations = product_locations_from_database(conn, product_name)

        # Load all locations for this product from the S3 inventory
        print(f'Loading locations from S3 Inventory')
        s3_locations = locations_from_inventory(prefix, suffix, inventory_dawg_file)

        # Compare and output
        not_in_db = dawg.CompletionDAWG(url
                                        for url in s3_locations.keys()
                                        if url not in db_locations)

        num_not_indexed = len(not_in_db.keys())

        missing_urls = [url for url in not_in_db.keys()]

        # Compare and output
        product_report = {
            'prefix_used': prefix,
            'suffix_used': suffix,
            'num_in_db': len(db_locations.keys()),
            'num_on_s3': len(s3_locations.keys()),
            'num_not_indexed': num_not_indexed,
            'not_indexed_by_year': count_by_year(not_in_db.keys()),
            'missing_urls': missing_urls
        }
        print(product_report)
        all_product_report[product_name] = product_report

    return all_product_report


def _yaml_or_json(line):
    return line.endswith('yaml') or line.endswith('json')


@click.command()
@click.option('--manifest-url', default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help='Base path to discover S3 Inventory')
@click.option('--postgresql-connection', help='libpq connection string, may be either key/value or a connection URL. '
                                              'See '
                                              'https://www.postgresql.org/docs/9.6/libpq-connect.html#LIBPQ-CONNSTRING'
                                              ' for details. May also be ommitted and connection details will come from'
                                              ' environment variables and .pgpass file.')
@click.option('--output-dir')
def main(manifest_url, postgresql_connection):
    manifest = load_inventory_manifest(manifest_url)
    urls = get_inventory_urls(manifest)
    for url in tqdm(urls, desc='Downloading S3 Inventory'):
        s3_download(url)

    d = dawg.CompletionDAWG(stream_inventory_parallel(predicate=_yaml_or_json, max_workers=2))
    s3_datasets_dawg = 'datasets_on_s3.dawg'
    d.save(s3_datasets_dawg)

    psql_conn = psycopg2.connect(postgresql_connection)
    prod_locations = find_product_locations(psql_conn)

    all_product_report = compare_all_products(prod_locations, psql_conn, s3_datasets_dawg)

    Path('all_products_report.json').write_text(json.dumps(all_product_report))


if __name__ == '__main__':
    main()
