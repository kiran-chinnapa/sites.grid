import json

import requests
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from crawlers.utils import grid_utils


def get_company_urls():
    distinct_urls = grid_utils.global_search(grid_id=grid_utils.qa_grid_id, auth_id=grid_utils.qa_auth_id,
                                             search_query=grid_utils.company_urls, search_type='distinct')
    return distinct_urls['matchingValues']


def get_all_career_links(company_url=None):
    career_links_dict = json.loads(grid_utils.get_career_links)
    career_links_dict['query']['columnFilter']['filters'][0]['keyword'] = company_url
    json_result = grid_utils.global_search(grid_id=grid_utils.qa_grid_id, auth_id=grid_utils.qa_auth_id,
                                           search_query=json.dumps(career_links_dict))
    return json_result['rows']


class JobsCrawlerSpider(scrapy.Spider):
    name = 'jobs_crawler'

    def start_requests(self):
        comp_urls = get_company_urls()
        # comp_urls = ['http://www.teksystems.com']
        i = 0
        for comp_url in comp_urls:
            i = i + 1
            pages = get_all_career_links(comp_url)
            # pages = [['https://www.teksystems.com/en/careers/internships'],['https://www.teksystems.com/en/careers/benefits'],
            #          ['https://www.teksystems.com/en/careers/internal-careers/experienced-professionals/global-services-careers'],
            #          ['https://www.teksystems.com/en/careers/internal-careers/students-recent-graduates']]
            page_url = pages[0][0]
            del pages[0]
            yield scrapy.Request(page_url, self.parse, meta={'pages': pages.copy()})
            # if i == 1:
            #     break

    def is_jobs_page(self, response):
        print(response.url)
        return False

    def parse(self, response):
        print('resp_url:{}'.format(response.url))
        print('resp_url:meta:{}'.format(response.meta))
        pages = response.meta['pages']
        if self.is_jobs_page(response):
            yield response.url
        elif pages:
            page_url = pages[0][0]
            del pages[0]
            yield scrapy.Request(page_url, self.parse, meta={'pages': pages.copy()})


if __name__ == '__main__':
    # process = CrawlerProcess(settings=get_project_settings())
    # process.crawl(JobsCrawlerSpider)
    # process.start()
    # urls = get_company_urls()
    # for url in urls:
    #     links = get_all_career_links(url)
    #     print(len(links[0]))
    #     print(links)
    #     print(links[1:])
    #     # print(links[1][0])
    #     break
    j = JobsCrawlerSpider()
    resp = requests.get('http://www.google.com')
    j.is_jobs_page(resp)
