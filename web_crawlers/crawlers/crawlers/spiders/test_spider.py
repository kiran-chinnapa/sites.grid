import scrapy


class TestSpider(scrapy.Spider):
    name = 'test'
    start_urls = ['https://www.google.com']

    def parse(self, response):
        print("dummy spider to start twisted server")

