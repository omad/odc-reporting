import json
import os.path
import re
from collections import Counter, defaultdict
from datetime import datetime
from itertools import groupby
from pathlib import Path

import click
import dawg
import psycopg2
from odc.aws import inventory
from odc.aws import s3_client
from psycopg2.extras import NamedTupleCursor

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterator, *args, **kwargs):
        return iterator


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

        if num_not_indexed > 0:
            print(f"Recording un-indexed URLs into {product_name}.txt")
            Path(f'{product_name}.txt').write_text('\n'.join(not_in_db.keys()) + '\n')

        # Compare and output
        product_report = {
            'prefix_used': prefix,
            'suffix_used': suffix,
            'num_in_db': len(db_locations.keys()),
            'num_on_s3': len(s3_locations.keys()),
            'num_not_indexed': num_not_indexed,
            'not_indexed_by_year': count_by_year(not_in_db.keys()),
            'on_s3_by_year': count_by_year(s3_locations.keys())
        }
        print(product_report)
        all_product_report[product_name] = product_report

    return all_product_report


def _dataset_files(inventory_stream):
    for record in inventory_stream:
        if (record.Key.endswith('yaml') or record.Key.endswith('json')) and not record.Key.endswith('.proc-info.yaml'):
            yield f's3://{record.Bucket}/{record.Key}'


@click.command()
@click.option('--manifest-url', default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help='Base path to discover S3 Inventory')
@click.option('--postgresql-connection', help='libpq connection string, may be either key/value or a connection URL. '
                                              'See '
                                              'https://www.postgresql.org/docs/9.6/libpq-connect.html#LIBPQ-CONNSTRING'
                                              ' for details. May also be ommitted and connection details will come from'
                                              ' environment variables and .pgpass file.',
              default='')
@click.option('--output-dir', default='.')
def main(output_dir, manifest_url, postgresql_connection):
    s3 = s3_client()
    manifest_url = inventory.find_latest_manifest(manifest_url, s3)
    print(f"Loading inventory from {manifest_url}")
    manifest_date = manifest_url.split('/')[-2]
    inventory_dawg = manifest_date + '_bucket_inventory.dawg'
    if not Path(inventory_dawg).exists():
        print(f"Streaming inventory into {inventory_dawg}")
        d = dawg.CompletionDAWG(_dataset_files(inventory.list_inventory(manifest_url, n_threads=4)))
        d.save(inventory_dawg)
    else:
        print(f"Found existing inventory dump in {inventory_dawg}")

    # Save as an absolute path before we change directories
    inventory_dawg = str(Path(inventory_dawg).absolute())

    psql_conn = psycopg2.connect(postgresql_connection)
    prod_locations = find_product_locations(psql_conn)

    print(f"Connected to Database {psql_conn.info.user}@{psql_conn.info.host}/{psql_conn.info.dbname}")
    print(f"Saving outputs into {output_dir}")
    Path(output_dir).mkdir(exist_ok=True)
    os.chdir(output_dir)
    all_product_report = compare_all_products(prod_locations, psql_conn, inventory_dawg)

    report = {
        'metadata': {
            'inventory_date': manifest_date,
            'inventory_manifest': manifest_url,
            'execution_date': datetime.now().isoformat(),
            'database': {
                'dbname': psql_conn.info.dbname,
                'user': psql_conn.info.user,
                'host': psql_conn.info.host
            }
        },
        'all_products': all_product_report
    }

    print(f'Saving report information into {manifest_date}_report.json')
    Path(f'{manifest_date}_report.json').write_text(json.dumps(report))


if __name__ == '__main__':
    main()
