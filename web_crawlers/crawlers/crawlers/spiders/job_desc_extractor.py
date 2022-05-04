import ast
import json
import logging
import re
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from crawlers.utils import keywords, grid_utils


class JobDescExtractor(Spider):
    name = 'job-desc-extractor'

    # constructor of the class with last two params as non-keyword argument(*args) and keyword argument(**kwargs).
    def __init__(self, *args, **kwargs):
        self.json_dict = json.loads(grid_utils.query_template)
        self.uncrawl_dict = {"insert": {"rows": []}}
        self.le = LinkExtractor(unique=True, process_value=None, deny_extensions=None, strip=True)
        super(JobDescExtractor, self).__init__(*args, **kwargs)
        logging.info("JobDescExtractor spider initialized")

    # def start_requests(self):
    #     comp_urls = grid_utils.get_company_urls(grid_utils.qa_jp_grid_id)
    #     for comp_url in comp_urls:
    #         logging.info('processing request: {}'.format(comp_url))
    #         if not grid_utils.is_row_present('Company Website', comp_url, self.grid_id) and not grid_utils.is_row_present(
    #                     'company_url', comp_url, self.uncrawl_grid):
    #             career_pages = grid_utils.get_all_rows(comp_url, grid_utils.qa_jp_grid_id)
    #             for career_row in career_pages:
    #                 meta_info = {'info': {'job_page_url': career_row['Job Posting Page'],
    #                                       'career_page_url': career_row['Career Page'],
    #                                       'company_name': career_row['Company Name'],
    #                                       'company_url': career_row['Company Website'],
    #                                       'dmv_grid_name': career_row['DMV Grid Name'],
    #                                       'dmv_grid_id': career_row['DMV Grid Id']
    #                                       }}
    #                 try:
    #                     logging.debug('queuing request: {}'.format(comp_url))
    #                     yield scrapy.Request(url=career_row['Job Posting Page'],
    #                               meta=meta_info, errback=self.errback)
    #                 except Exception as e:
    #                     self.add_to_uncrawled(meta_info, str(e))


    # debug purpose
    def start_requests(self):
        with open('jp_links.jl') as reader:
            job_page_rows = ast.literal_eval(reader.read())
            for job_page_row in job_page_rows:
                meta_info = {'info': {'job_page_url': job_page_row['Job Posting Page'],
                                      'career_page_url': job_page_row['Career Page'],
                                      'company_name': job_page_row['Company Name'],
                                      'company_url': job_page_row['Company Website'],
                                      'dmv_grid_name': job_page_row['DMV Grid Name'],
                                      'dmv_grid_id': job_page_row['DMV Grid Id']
                                      }}
                try:
                    yield scrapy.Request(url=job_page_row['Job Posting Page'],
                              meta=meta_info, errback=self.errback)
                except Exception as e:
                    self.add_to_uncrawled(meta_info, str(e))


    def parse(self, response):
        # print('urls:{}'.format(response.url))
        all_urls = self.get_all_links(response)
        if len(all_urls) > 0:
            for link_url in all_urls:
                # print('urls not crawled:{}'.format(link_url))
                # print(response.request.headers['User-Agent'])
                # not_crawled.append(link_url)
                yield scrapy.Request(url=link_url, callback=self.parse_job_desc, meta=response.meta, errback=self.errback)
                # break


    def parse_job_desc(self, response):
        # title = response.css('title::text').extract()
        # print('title::{}'.format(title))
        if re.search(keywords.JOB_DESC, response.text, re.IGNORECASE):
            yield {
                'Job Details Page': response.url,
                'Job Posting Page': response.meta['info']['job_page_url'],
                'Career Page URL': response.meta['info']['career_page_url'],
                'Company Website': response.meta['info']['company_url'],
                'DMV Grid Name': response.meta['info']['dmv_grid_name'],
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
        meta_info['info']['Spider Name'] = JobDescExtractor.name
        self.uncrawl_dict["insert"]["rows"].append(meta_info['info'])
        grid_utils.add_row(self.uncrawl_grid, grid_utils.qa_auth_id, self.uncrawl_dict)
        self.uncrawl_dict["insert"]["rows"] = []


if __name__ == '__main__':
    crawl_settings = get_project_settings()
    # configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
    # crawl_settings['FEED_FORMAT'] = 'json'
    # crawl_settings['FEED_URI'] = 'result.json'
    # crawl_settings['FEEDS'] = {'job_desc.links.jl': {'format': 'json'}}
    # crawl_settings['FEEDS'] = {'company.links.jl': {'format': 'json'}}
    # crawl_settings['LOG_LEVEL'] = 'INFO'
    crawl_settings['FEEDS'] = {
        "job-desc-links.jl": {"format": "json"},
    }
    crawl_settings['ITEM_PIPELINES'] = {}
    process = CrawlerProcess(settings=crawl_settings)
    process.crawl(JobDescExtractor, grid_id=grid_utils.qa_job_desc_grid_id,uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Job Details Page')
    process.start()
    # print(not_crawled)

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
