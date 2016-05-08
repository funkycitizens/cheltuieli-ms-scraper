from contextlib import contextmanager
import csv
import re
import scrapy
from scrapy.crawler import CrawlerProcess

@contextmanager
def write_csv(name):
    with open('out/%s' % name, 'wb') as f:
        csv_file = csv.writer(f)
        def writerow(row):
            csv_file.writerow([c.encode('utf-8') for c in row])
        yield writerow

def all_text(node):
    return ' '.join(node.css('::text').extract()).strip()

def table_rows(table):
    for tr in table.css('tr'):
        row = []
        for td in tr.css('td'):
            row.append(all_text(td))
            colspan = td.css('::attr(colspan)').extract_first()
            for _ in xrange(int(colspan or 1) - 1):
                row.append('')
        yield row

class CheltuieliSpider(scrapy.Spider):
    name = 'cheltuieli'

    def get_page(self, n):
        return scrapy.FormRequest(
            'http://www.monitorizarecheltuieli.ms.ro/centralizator',
            formdata={
                'page': str(n),
                'cautare_text': '',
                'data_filtrare': '2016-01',
            },
            callback=self.results_page,
        )

    def start_requests(self):
        yield self.get_page(1)

    def results_page(self, resp):
        for tr in resp.css('.records tr')[1:]:
            hospital = all_text(tr.css('td')[0])
            [href] = tr.css('td')[1].css('a::attr(href)').extract()
            yield scrapy.Request(
                resp.urljoin(href),
                callback=self.form1,
                meta={'hospital': hospital},
            )

        onclick = (
            resp
            .xpath('//a[contains(text(), "Next")]')
            .css('::attr(onclick)')
            .extract_first()
        )
        if onclick:
            m = re.match(r'^populare_form\((\d+)\);$', onclick)
            next_page = int(m.group(1))
            yield self.get_page(next_page)

    def form1(self, resp):
        COLS = 9
        id = int(resp.url.split('/')[-1])
        with write_csv('%d.csv' % id) as writerow:
            writerow([resp.meta['hospital']] + [''] * (COLS - 1))
            for row in table_rows(resp.css('table table')[0]):
                assert len(row) == COLS
                writerow(row)

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
})

process.crawl(CheltuieliSpider)
process.start()
