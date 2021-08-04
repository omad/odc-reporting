
.all: product_indexing_report.pdf product_indexing_report.html

product_indexing_report.pdf: product_indexing_report.md
#	pandoc --pdf-engine wkhtmltopdf --from markdown+pipe_tables+table_captions --toc -o $@ $<
	pandoc --pdf-engine xelatex \
		-V geometry:"top=2cm, bottom=1.5cm, left=2cm, right=2cm, a4paper" \
		-V mainfont="Bitstream Vera Sans" \
		--from markdown --toc -o $@ $<
#		-V mainfont="Bitstream Charter" \
#	pandoc -c pandoc.css --pdf-engine wkhtmltopdf --from markdown+pipe_tables+table_captions --toc -o $@ $<

new.md: product_indexing_report.j2 2021-08-03T01-00Z_all_products_report.json
	j2 -f json $^ > $@

eisvogel.pdf: product_indexing_report.md eisvogel.tex
	pandoc $< -o $@ --from markdown --toc --template eisvogel.tex

product_indexing_report.html: product_indexing_report.md
	pandoc -s --from markdown+pipe_tables+table_captions --toc -o $@ $<

product_indexing_report.md: product_indexing_report.j2 all_products_report.json
	j2 -f json $^ > $@

test.pdf: test.md
	pandoc --toc --pdf-engine wkhtmltopdf --toc -o test.pdf test.md


report.pdf: product_indexing_report.md
	pandoc                          \
	  --toc							\
	  --from         markdown       \
	  --to           latex          \
	  --template     template.tex   \
	  --out          report.pdf 	\
	  --pdf-engine   xelatex        \
	  $<
