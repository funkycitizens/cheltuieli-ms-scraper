import scrapy
from scrapy.crawler import CrawlerProcess

def all_text(node):
    return ' '.join(node.css('::text').extract()).strip()

class CheltuieliSpider(scrapy.Spider):
    name = 'cheltuieli'

    def start_requests(self):
        yield scrapy.FormRequest(
            'http://www.monitorizarecheltuieli.ms.ro/centralizator',
            formdata={
                'page': '1',
                'cautare_text': '',
                'data_filtrare': '2016-01',
            },
            callback=self.home,
        )

    def home(self, resp):
        for tr in resp.css('.records tr')[1:]:
            hospital = all_text(tr.css('td')[0])
            [href] = tr.css('td')[1].css('a::attr(href)').extract()
            yield scrapy.Request(
                resp.urljoin(href),
                callback=self.form1,
                meta={'hospital': hospital},
            )

    def form1(self, resp):
        print resp.meta['hospital']
        print resp.body_as_unicode()

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
})

process.crawl(CheltuieliSpider)
process.start()
