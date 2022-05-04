import os
import time
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings



class RecursiveSpider(CrawlSpider):
    name = 'r-spider'

    def start_requests(self):
        urls = [
            'http://www.1901group.com/careers-jobs/',
            'https://2hb.catsone.com/careers/',
            'https://www.3clogic.com/careers/#search'
                ]
        for url in urls:
            yield scrapy.Request(url)

    rules = (
        Rule(LinkExtractor(allow='careers|jobs|search'), callback='parse_item'),
        # Rule(LinkExtractor(allow='page'), callback='parse_item')
    )

    def parse_item(self, response):
        yield {
            'sub-link': response.url
        }


if __name__ == '__main__':
    # contains_job_titles('https://www.experis.com/en/careers')
    process = CrawlerProcess(get_project_settings())
    process = CrawlerProcess(settings={
        "FEEDS": {
            "sub-links.jl": {"format": "json"},
        },
        "DOWNLOADER_MIDDLEWARES": {
            'crawlers.middlewares.JSMiddleware': 543,
        }
    })
    process.crawl(RecursiveSpider)
    process.start()
