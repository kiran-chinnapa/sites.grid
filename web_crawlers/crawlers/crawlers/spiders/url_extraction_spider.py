# source https://medium.com/@chetaniam/using-scrapy-to-create-a-generic-and-scalable-crawling-framework-83d36732181
# https://docs.scrapy.org/en/latest/topics/link-extractors.html?highlight=LxmlLinkExtractor#scrapy.linkextractors.lxmlhtml.LxmlLinkExtractor
import logging
import sys

from scrapy.spiders import Spider
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from crawlers.utils import keywords, grid_utils
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess
import traceback


class UrlExtractorSpider(Spider):
    name = 'url-extractor'
    start_urls = []

    # constructor of the class with last two params as non-keyword argument(*args) and keyword argument(**kwargs).
    def __init__(self, *args, **kwargs):
        self.uncrawl_dict = {"insert": {"rows": []}}
        self.le = LinkExtractor(
            allow=keywords.CAREER_PAGE,
            unique=True, process_value=self.process_value,
            deny_extensions=None, strip=True)
        self.case_sensitive_dict = {}
        super(UrlExtractorSpider, self).__init__(*args, **kwargs)
        logging.info("UrlExtractorSpider spider initialized")

    def process_value(self, value):
        for v in value:
            if v.isupper():
                self.case_sensitive_dict[value.lower()] = value
                break
        return value.lower()

    def start_requests(self):
        meta_info = {'info': self.companies}
        try:
            yield Request(self.companies['company_url'], meta=meta_info, errback=self.errback)
        except Exception as e:
            self.add_to_uncrawled(meta_info, str(e))

    def errback(self, failure):
        self.add_to_uncrawled(failure.request.meta, '{}:{}'.format(str(failure.type)[1:-1], failure.value))

    def add_to_uncrawled(self, meta_info, error_reason):
        # if not grid_utils.is_row_present('company_url', meta_info['info']['company_url'], self.uncrawl_grid):
        meta_info['info']['Error Reason'] = error_reason
        meta_info['info']['Spider Name'] = UrlExtractorSpider.name
        self.uncrawl_dict["insert"]["rows"].append(meta_info['info'])
        grid_utils.add_row(self.uncrawl_grid, grid_utils.qa_auth_id, self.uncrawl_dict)
        self.uncrawl_dict["insert"]["rows"] = []

    def parse(self, response):
        all_urls = self.get_all_links(response)
        if len(all_urls) > 0:
            found = False
            for url in all_urls:
                if 'career' in url:
                    c_url = self.case_sensitive_dict.get(url)
                    if c_url is not None: url = c_url
                    yield {
                        'Career Page': url,
                        'Company Website': response.meta['info']['company_url'],
                        'Company Name': response.meta['info']['company_name'],
                        'DMV Grid Name': response.meta['info']['dmv_grid_name'],
                        'DMV Grid Id': response.meta['info']['dmv_grid_id'],
                    }
                    found = True
                    break
            if not found:
                for url in all_urls:
                    tokens = url.split('/')
                    if '-' in tokens[len(tokens) - 1]: continue
                    c_url = self.case_sensitive_dict.get(url)
                    if c_url is not None: url = c_url
                    yield {
                        'Career Page': url,
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


def read_from_top_companies():
    try:
        crawl_settings = get_project_settings()
        # crawl_settings['TELNETCONSOLE_ENABLED'] = False
        crawl_settings['FEEDS'] = {
            "company.links.jl": {"format": "json"},
        }
        crawl_settings['ITEM_PIPELINES'] = {}
        crawl_settings['DOWNLOADER_MIDDLEWARES'] = {
            'crawlers.middlewares.JSMiddleware': 543,
        }
        process = CrawlerProcess(settings=crawl_settings)
        with open('../resources/top_companies') as f:
            list_iterable = iter(f.readlines())
            # next(list_iterable)
            for line in list_iterable:
                company_meta = {'company_url': line.rstrip('\n') if line.endswith('\n') else line, 'company_name': "",
                                'dmv_grid_name': "", 'dmv_grid_id': ""}
                logging.info('crawling company {}'.format(company_meta['company_url']))
                process.crawl(UrlExtractorSpider, grid_id=grid_utils.qa_top_companies_grid,
                              uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
                              companies=company_meta)
        process.start()
    except:
        traceback.print_exc()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('please pass input args (top_companies, dmv_grids) for spider')
    elif 'top_companies' == sys.argv[1]:
        read_from_top_companies()
    elif 'dmv_grids' == sys.argv[1]:
        # read_from_dmv_grids()
        pass

# if __name__ == '__main__':
#     crawl_settings = get_project_settings()
#     # crawl_settings['LOG_LEVEL'] = 'ERROR'
#     crawl_settings['FEEDS'] = {
#         "links.jl": {"format": "json"},
#     }
#     crawl_settings['ITEM_PIPELINES'] = {}
#     # crawl_settings['PROXY_POOL_ENABLED'] = True
#     # crawl_settings['DOWNLOADER_MIDDLEWARES'] = {
#     # # ...
#     # 'scrapy_proxy_pool.middlewares.ProxyPoolMiddleware': 610,
#     # 'scrapy_proxy_pool.middlewares.BanDetectionMiddleware': 620,
#     # # ...
#     # }
#     # crawl_settings['ROBOTSTXT_OBEY'] = False
#     company_meta = {'company_url': sys.argv[1],
#                     'company_name': "",
#                     'dmv_grid_name': "",
#                     'dmv_grid_id': ""}
#     process = CrawlerProcess(settings=crawl_settings)
#     process.crawl(UrlExtractorSpider, companies=company_meta)
#     process.start()
