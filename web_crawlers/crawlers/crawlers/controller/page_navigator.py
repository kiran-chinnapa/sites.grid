import os
import re
import sys
import time
import traceback
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from crawlers.controller.job_metadata_parser import JobMetadataParser
from crawlers.utils import grid_utils
import logging
from selenium.webdriver.common.by import By


class PageNavigator:
    def __init__(self, driver, jp_row):
        self.jp_row = jp_row
        self.links_filter = r"/[0-9]+/|from=|page="
        self.page_rgx = r"from=|page="
        self.le = LinkExtractor(unique=True, deny_extensions=None, strip=True)
        self.pending_pages = []
        self.crawled_pages = []
        self.driver = driver
        self.job_metadata_parser = JobMetadataParser(driver)
        self.insertDataDict = {"insert": {"rows": []}}
        self.links_size = 0
        self.running = True

    def get_next_page(self, all_links, current_page):
        next_page = None
        buttons = [".search-results-load-more-btn"]
        amz_buttons = [".right"]
        link_text = ["Next"]
        if "pg=" in self.jp_row['Job Posting Page']:
            pg_num = int(current_page[current_page.find('pg=') + 3])
            page_link = '{}{}{}'.format(current_page[:current_page.find('pg=') + 3], pg_num + 1,
                                        current_page[current_page.find('pg=') + 4:])
            self.driver.get(page_link)
            time.sleep(1)
            page_link = self.driver.current_url
            if page_link not in self.crawled_pages:
                self.crawled_pages.append(page_link)
                next_page = page_link
        elif "oraclecloud" in self.jp_row['Job Posting Page']:
            self.click_after_scroll(buttons)
            if len(all_links) != self.links_size:
                next_page = self.jp_row['Job Posting Page']
            self.links_size = len(all_links)
            logging.info('links size: {}'.format(self.links_size))
        elif "amazon" in self.jp_row['Job Posting Page']:
            self.click_next_button(amz_buttons)
            if self.driver.current_url not in self.crawled_pages:
                self.crawled_pages.append(self.driver.current_url)
                next_page = self.driver.current_url
        else:
            self.click_link_text(link_text)
            if self.driver.current_url not in self.crawled_pages:
                self.crawled_pages.append(self.driver.current_url)
                next_page = self.driver.current_url
        if next_page is None:
            for link in all_links:
                if '#' in link:
                    link = link.split('#')[0]
                if re.search(self.page_rgx, link) and link not in self.crawled_pages and link not in self.pending_pages:
                    self.pending_pages.append(link)
            if self.pending_pages:
                next_page = self.pending_pages[0]
                del self.pending_pages[0]
                self.crawled_pages.append(next_page)

        return next_page

    def process(self):
        try:
            page_link = self.jp_row['Job Posting Page']
            all_links = []
            cnt = 0
            while page_link is not None:
                if not self.running:
                    raise Exception("long running thread {} terminated".format(self.jp_row['Job Posting Page']))
                all_links.clear()
                all_links.extend(self.get_all_links(page_link))
                out_json, cnt = self.process_jobs(all_links, cnt)
                if not bool(out_json) and cnt == 0:
                    logging.info("no jobs found for {}".format(self.jp_row['Job Posting Page']))
                    raise Exception("no jobs found for {}".format(self.jp_row['Job Posting Page']))
                page_link = self.get_next_page(all_links, page_link)
                logging.info("navigation page:{}".format(page_link))
                cnt += 1
        except Exception as e:
            raise Exception(str(e))

    def click_link_text(self, texts):
        try:
            for text in texts:
                time.sleep(1)
                elements = self.driver.find_elements(By.LINK_TEXT, text)
                if not elements:
                    self.driver.refresh()
                    time.sleep(2)
                    elements = self.driver.find_elements(By.LINK_TEXT, text)
                if elements:
                    elements[0].click()
                    time.sleep(2)
        except:
            logging.error(traceback.format_exc())

    def click_next_button(self, buttons):
        try:
            time.sleep(1)
            for button in buttons:
                elements = self.driver.find_elements(By.CSS_SELECTOR, button)
                if elements:
                    elements[0].click()
                    time.sleep(2)
        except:
            logging.error(traceback.format_exc())

    def click_after_scroll(self, buttons):
        for button in buttons:
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(5)
                self.driver.execute_script("window.scrollTo(document.body.scrollHeight,500)")
                time.sleep(5)
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                while True:
                    self.driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
                    time.sleep(5)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                time.sleep(5)
                elements = self.driver.find_elements(By.CSS_SELECTOR, button)
                if elements:
                    elements[0].click()
                    logging.info('button clicked after scroll:{}'.format(button))
            except:
                logging.error(traceback.format_exc())

    def process_jobs(self, all_links, cnt):
        out_json = {}
        for link in all_links:
            if grid_utils.is_row_present({'Job Page': link},grid_utils.latest_jobs_grid_id, grid_utils.qa_auth_id, 'qa'):
                cnt += 1
                continue
            if self.is_valid_job_url(link):
                out_json.clear()
                out_json = self.jp_row.copy()
                out_json["Job Title"] = self.get_job_title(link)
                self.job_metadata_parser.map_job_columns(link, out_json)
                self.insertDataDict["insert"]["rows"].append(out_json)
                grid_utils.add_row(grid_utils.latest_jobs_grid_id, grid_utils.qa_auth_id, self.insertDataDict,env='qa')
                self.insertDataDict["insert"]["rows"].clear()
        return out_json,cnt

    def get_scrapy_response(self, url):
        try:
            if self.driver.current_url != url:
                self.driver.get(url)
                time.sleep(5)
            body = self.driver.page_source
            return HtmlResponse(url=self.driver.current_url, body=body, encoding='utf-8')
        except Exception as e:
            raise Exception('chrome_driver unable to process url :{} exception :{}'.format(url, str(e)))

    def get_all_links(self, parent_link):
        resp = self.get_scrapy_response(parent_link)
        links = self.le.extract_links(resp)
        str_links = []
        for link in links:
            str_links.append(link.url)
        return str_links

    def is_valid_job_url(self, link):
        tokens = link.split('/')
        for token in tokens:
            if '-' in token or token.isnumeric():
                return True
        return False

    def get_job_title(self, link):
        tokens = link.split('/')
        jt = None
        for token in tokens:
            if '-' in token:
                jt = token
                break
        return jt


if __name__ == '__main__':
    chrome_driver_path = os.getenv("chrome_driver_path")
    if chrome_driver_path is None:
        logging.error('please set environment variable chrome_driver_path to /Users/msk/drivers/chromedriver on mac')
        sys.exit(0)
    chrome_options = Options()
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.headless = True
    s = Service(chrome_driver_path)
    chrome_driver = webdriver.Chrome(service=s, options=chrome_options)
    target_posting_pages = [
        # 'https://careers.microsoft.com/us/en/search-results',
        # 'https://www.facebookcareers.com/jobs',
        # 'https://jobs.apple.com/en-us/search?location=united-states-USA',
        # 'https://www.accenture.com/us-en/careers/jobsearch?jk=&sb=1&pg=1&is_rj=0'
        # 'https://www.amazon.jobs/en/job_categories/software-development',
        # 'https://careers.bankofamerica.com/en-us/job-search?ref=search&start=0&rows=10&search=getAllJobs',
        # 'https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions',
        # 'https://hdpc.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions?sortBy=POSTING_DATES_DESC',

    ]
    for page in target_posting_pages:
        page_nav = PageNavigator(chrome_driver, {'Job Posting Page': page})
        page_nav.process()
    chrome_driver.quit()
