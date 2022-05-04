import json
import logging
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from crawlers.utils import keywords, grid_utils


class JPLinkExtractor(Spider):
    name = 'jp-link-extractor'

    # constructor of the class with last two params as non-keyword argument(*args) and keyword argument(**kwargs).
    def __init__(self, *args, **kwargs):
        self.json_dict = json.loads(grid_utils.query_template)
        self.uncrawl_dict = {"insert": {"rows": []}}
        self.le = LinkExtractor(allow=keywords.JOBS_PAGES, unique=True, process_value=self.process_value,
                                deny_extensions=None, strip=True)
        super(JPLinkExtractor, self).__init__(*args, **kwargs)
        logging.info("JPLinkExtractor spider initialized")

    def process_value(self, value):
        return value.lower()

    def start_requests(self):
        comp_urls = grid_utils.get_company_urls(grid_utils.qa_career_grid_id)
        for comp_url in comp_urls:
            logging.info('processing request: {}'.format(comp_url))
            if not grid_utils.is_row_present({'Company Website': comp_url}, self.grid_id) and not grid_utils.is_row_present(
                    {'company_url': comp_url}, self.uncrawl_grid):
                career_pages = grid_utils.get_all_rows(comp_url,grid_utils.qa_career_grid_id)
                for career_row in career_pages:
                    meta_info = {'info': {'career_page_url': career_row['Career Page URL'],
                                 'company_url': career_row['Company Website'],
                                 'company_name': career_row['Company Name'],
                                 'dmv_grid_name': career_row['DMV Grid Name'],
                                 'dmv_grid_id': career_row['DMV Grid Id']}}
                    try:
                        logging.debug('queuing request: {}'.format(comp_url))
                        yield Request(career_row['Career Page URL'],
                                      meta=meta_info, errback=self.errback)
                    except Exception as e:
                        self.add_to_uncrawled(meta_info, str(e))

    # for debugging purpose
    # def start_requests(self):
    #     career_row = {"Career Page URL": "http://careers.leidos.com/1901", "Company Website": "http://1901group.com",
    #                   "Company Name": "1901 Group", "DMV Grid Name": "IT_Staffing_DMV.grid",
    #                   "DMV Grid Id": "58c6f4d8c097d64efa264c39/share/5cf94950c9d08233ceac2977"}
    #     meta_info = {'info': {'career_page_url': career_row['Career Page URL'],
    #                           'company_url': career_row['Company Website'],
    #                           'company_name': career_row['Company Name'],
    #                           'dmv_grid_name': career_row['DMV Grid Name'],
    #                           'dmv_grid_id': career_row['DMV Grid Id']}}
    #     try:
    #         yield Request(career_row['Career Page URL'],
    #                       meta=meta_info, errback=self.errback)
    #     except Exception as e:
    #         self.add_to_uncrawled(meta_info, str(e))

    def parse(self, response):
        all_urls = self.get_all_links(response)
        if len(all_urls) > 0:
            for url in all_urls:
                yield {
                    'Job Posting Page': url,
                    'Career Page': response.meta['info']['career_page_url'],
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

    def errback(self, failure):
        self.add_to_uncrawled(failure.request.meta, '{}:{}'.format(str(failure.type)[1:-1], failure.value))

    def add_to_uncrawled(self, meta_info, error_reason):
        # if not grid_utils.is_row_present('company_url', meta_info['info']['company_url'], self.uncrawl_grid):
        meta_info['info']['Error Reason'] = error_reason
        meta_info['info']['Spider Name'] = JPLinkExtractor.name
        self.uncrawl_dict["insert"]["rows"].append(meta_info['info'])
        grid_utils.add_row(self.uncrawl_grid, grid_utils.qa_auth_id, self.uncrawl_dict)
        self.uncrawl_dict["insert"]["rows"] = []


if __name__ == '__main__':
    crawl_settings = get_project_settings()
    # configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
    # crawl_settings['FEED_FORMAT'] = 'json'
    # crawl_settings['FEED_URI'] = 'result.json'
    # crawl_settings['FEEDS'] = {'page.links.jl': {'format': 'json'}}
    crawl_settings['FEEDS'] = {'jp_links.jl': {'format': 'json'}}
    # crawl_settings['LOG_LEVEL'] = 'INFO'
    crawl_settings['ITEM_PIPELINES'] = {}
    process = CrawlerProcess(settings=crawl_settings)
    process.crawl(JPLinkExtractor, grid_id=grid_utils.qa_jp_grid_id, uncrawl_grid=grid_utils.qa_uncrawl_grid_id,
                  unique_column='Job Posting Page')
    process.start()

    # runner = CrawlerRunner(settings=crawl_settings)
    # for root_url in ['http://www.roberthalf.com','http://www.teksystems.com']:
    # for root_url in ['http://www.intepros.com/']:
    # if 'www.' in root_url:
    #     domain = root_url.split('www.')[1]
    # else:
    #     domain = root_url.split('/')[2]
    # runner.crawl(UrlExtractor, root=root_url, depth=0, allow=keywords.CAREER_PAGE, deny='-')
    # d = runner.join()
    # d.addBoth(lambda _: reactor.stop())

    # reactor.run()  # the script will block here until all crawling jobs are finished
