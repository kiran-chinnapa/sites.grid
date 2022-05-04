import traceback
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from crawlers.items import JobsItem
from scrapy import signals


class ErrorSpider(scrapy.Spider):
    name = 'jobs'
    # start_urls = []
    urls =[]

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     # spider = super(JobsSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(cls.request_dropped, signal=signals.request_dropped)
    #
    # def request_dropped(self, request, spider):
    #     '''handle failed url (request.url)'''
    #     print('handling dropped request')
    #     pass

    def start_requests(self):
        for url in ErrorSpider.urls:
            try:
                # print('processing : {}'.format(url))
                yield scrapy.Request(url, errback=self.errback)
            except Exception as e:
                self.parse_value_err('resulting error: {}:{}'.format(url,e))

    def parse_value_err(self, error_details):
        print(error_details)

    def errback(self, failure):
        '''handle failed url (failure.request.url)'''
        print('resulting error: {}'.format(failure.request.url))

    def parse(self, response):
        # item = JobsItem()
        # # yield {'scraped_url': response.url}
        # item['career_page_url'] = response.url
        # item['job_tile'] = response.css('title::text').get()
        # item['salary'] = len(response.css('title::text').get())
        # item['notice_period'] = len(response.css('title::text').get()) + 30
        # yield item
        print('resulting success: {}'.format(response.url))

if __name__ == '__main__':
    crawl_settings = get_project_settings()
    # crawl_settings['LOG_LEVEL'] = 'CRITICAL'
    process = CrawlerProcess(crawl_settings)
    ErrorSpider.urls.extend([
        'http://www.bashdigital.com/',
        # 'https://careers.jdfldfl.com/us/en',
        # 'http://www.fpmi.com/'
        # , 'blah blah', 'http://www.teksystems.com'
        # 'http://www.insight-hq.com'
    ])
    process.crawl(ErrorSpider)
    process.start()
