import os
import re

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from crawlers.utils import grid_utils, keywords

'''read all job pages from job pages grid for each company, once you get the job titles page print the row and skip other rows
logic for getting job titles page is to check for JOB_TITLE regex on page and having more than 5 hrefs'''


class JobTitleSpider(scrapy.Spider):
    name = 'job-title'

    def start_requests(self):
        company_list = grid_utils.get_company_urls(grid_utils.qa_jp_grid_id)
        for company in company_list:
            job_page_rows = grid_utils.get_all_rows(company, grid_utils.qa_jp_grid_id)
            for job_page_row in job_page_rows:
                if self.contains_job_titles(job_page_row['Job Posting Page']):
                    print('found job page: {}'.format(job_page_row))
                    print('********************************************************************')
                    break
        # urls = ['http://quotes.toscrape.com/']
        # for url in urls:
        #     yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        title = response.css('title::text').extract()
        yield {'titletext': title}
        # following links in page
        # response.follow(next_page,callback=self.parse)
        # how to use pagination
        # response.follow(pagenumber_string_start_url,callback=self.parse)

    def contains_job_titles(self, job_page_url):
        chrome_driver_path = os.getenv("chrome_driver_path")
        chrome_options = Options()
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.headless = True
        s = Service(chrome_driver_path)
        driver = webdriver.Chrome(service=s, options=chrome_options)
        driver.get(job_page_url)
        body = driver.page_source
        hrefs = driver.find_elements(By.TAG_NAME, "a")
        if len(hrefs) > 5 and re.search(keywords.JOB_TITLE, body, re.IGNORECASE):
            for href in hrefs:
                print(href.get_attribute('href'))
            return True
        return False


if __name__ == '__main__':
    crawl_settings = get_project_settings()
    crawl_settings['LOG_LEVEL'] = 'ERROR'
    process = CrawlerProcess(crawl_settings)
    process.crawl(JobTitleSpider)
    process.start()
