# source https://medium.com/@chetaniam/using-scrapy-to-create-a-generic-and-scalable-crawling-framework-83d36732181
# https://docs.scrapy.org/en/latest/topics/link-extractors.html?highlight=LxmlLinkExtractor#scrapy.linkextractors.lxmlhtml.LxmlLinkExtractor
# Shuts down after running for couple of hours, hence cannot be used on serer.
import json
import logging
import os
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from crawlers.utils import keywords, grid_utils
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service


class JsUrlExtractorSpider(Spider):

    name = 'js-url-extractor'
    start_urls = []

    # constructor of the class with last two params as non-keyword argument(*args) and keyword argument(**kwargs).
    def __init__(self, *args, **kwargs):
        self.json_dict = json.loads(grid_utils.query_template)
        self.uncrawl_dict = {"insert": {"rows": []}}
        self.le = LinkExtractor(allow=keywords.CAREER_PAGE, unique=True, process_value=self.process_value, deny_extensions=None, strip=True)
        chrome_driver_path = os.getenv("chrome_driver_path")
        if chrome_driver_path is None:
            print('please set environment variable chrome_driver_path to /Users/msk/drivers/chromedriver on mac')
            return
        chrome_options = Options()
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.headless = True
        s = Service(chrome_driver_path)
        self.driver = webdriver.Chrome(service=s, options=chrome_options)
        super(JsUrlExtractorSpider, self).__init__(*args, **kwargs)
        logging.info("UrlExtractorSpider spider initialized")

    def process_value(self, value):
        return value.lower()

    def start_requests(self):
        uncrawled_companies = grid_utils.get_uncrawled_companies()
        for uncrawled_company in uncrawled_companies:
            company_website = uncrawled_company['company_url'].strip()
            if not company_website.startswith('http'):
                company_website = 'http://{}'.format(company_website)
            # company_website = 'https://www.cogniertechnology.com/'
            logging.info('processing request: {}'.format(company_website))
            if not grid_utils.is_row_present({'Company Website': company_website}, self.grid_id) and not grid_utils.is_row_present(
                    {'company_url': company_website}, self.uncrawl_grid):
                meta_info = {'info': {'company_url': company_website,
                                      'company_name': uncrawled_company['company_name'],
                                      'dmv_grid_name': uncrawled_company['dmv_grid_name'],
                                      'dmv_grid_id': uncrawled_company['dmv_grid_id']}}
                try:
                    logging.debug('queuing request: {}'.format(company_website))
                    yield Request(company_website,meta=meta_info, errback=self.errback)
                except Exception as e:
                    self.add_to_uncrawled(meta_info, str(e))
            # break

    def errback(self, failure):
        self.add_to_uncrawled(failure.request.meta, '{}:{}'.format(str(failure.type)[1:-1], failure.value))

    def add_to_uncrawled(self, meta_info, error_reason):
        # if not grid_utils.is_row_present('company_url', meta_info['info']['company_url'], self.uncrawl_grid):
        meta_info['info']['Error Reason'] = error_reason
        meta_info['info']['Spider Name'] = JsUrlExtractorSpider.name
        self.uncrawl_dict["insert"]["rows"].append(meta_info['info'])
        grid_utils.add_row(self.uncrawl_grid, grid_utils.qa_auth_id, self.uncrawl_dict)
        self.uncrawl_dict["insert"]["rows"] = []

    def parse(self, response):
        all_urls = self.get_all_links(response)

        if len(all_urls) > 0:
            for url in all_urls:
                # uncomment below two lines for debugging
                # print(url)
                yield {
                    'Career Page URL': url,
                    'Company Website': response.meta['info']['company_url'],
                    'Company Name': response.meta['info']['company_name'],
                    'DMV Grid Name': response.meta['info']['dmv_grid_name'],
                    'DMV Grid Id': response.meta['info']['dmv_grid_id'],
                }

    def get_all_links(self, response):
        links = self.le.extract_links(response)
        str_links = []
        for link in links:
            str_links.append(link.url)
        return str_links


if __name__ == '__main__':
    crawl_settings = get_project_settings()
    # crawl_settings['LOG_LEVEL'] = 'INFO'
    # crawl_settings['FEEDS'] = {
    #     "sub-links.jl": {"format": "json"},
    # }
    # crawl_settings['ITEM_PIPELINES'] = {}
    crawl_settings['ROBOTSTXT_OBEY'] = False
    crawl_settings['DOWNLOADER_MIDDLEWARES'] = {
        'crawlers.middlewares.JSMiddleware': 543,
    }
    process = CrawlerProcess(settings=crawl_settings)
    process.crawl(JsUrlExtractorSpider, grid_id=grid_utils.qa_career_grid_id, uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page URL')
    process.start()