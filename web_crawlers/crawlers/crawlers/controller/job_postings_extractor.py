import os
import re
import sys
import threading
import time
import traceback
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from crawlers.controller import job_metadata_parser
from crawlers.utils import grid_utils, keywords
import html2text
from selenium.webdriver.common.by import By
import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


class JobPostingsExtractor(threading.Thread):

    def __init__(self, cp_row=None, output_grid = grid_utils.qa_job_posting_page_grid, thread_id=0):
        self.spider_name = 'job-postings-extractor'
        chrome_driver_path = os.getenv("chrome_driver_path")
        if chrome_driver_path is None:
            logging.error('please set environment variable chrome_driver_path to /Users/msk/drivers/chromedriver on mac')
            sys.exit(0)
        chrome_options = Options()
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.headless = True
        s = Service(chrome_driver_path)
        self.driver = webdriver.Chrome(service=s, options=chrome_options)
        self.thread_id = thread_id
        self.cp_row = cp_row
        self.posting_regex_1 = r"/search-results|/jobs|/job-search|/search|/job_categories/|/jobsearch|.oraclecloud.|joblistpage|posting|apply"
        self.posting_regex_2 = keywords.JOBS_PAGES
        self.banking_regex = r"/requisitions"
        self.insert_row = {"insert": {"rows": []}}
        self.le = LinkExtractor(unique=True, deny_extensions=None, strip=True)
        self.page_links = []
        self.jp_link = None
        self.uncrawl_dict = {"insert": {"rows": []}}
        self.output_grid = output_grid
        self.job_meta_parser = job_metadata_parser.JobMetadataParser(self.driver)
        threading.Thread.__init__(self)

    def get_scrapy_response(self, url):
        try:
            if url != self.driver.current_url:
                self.driver.get(url)
            time.sleep(5)
            body = self.driver.page_source
            return HtmlResponse(url=self.driver.current_url, body=body, encoding='utf-8')
        except Exception as e:
            raise Exception('chrome_driver unable to process url :{} exception :{}'.format(url, str(e)))

    def filter_links(self, regex):
        links = []
        for link in self.page_links:
            # logging.info('page link: {}'.format(link.url))
            if re.search(regex, link.url, re.IGNORECASE):
                links.append(link.url)
                logging.info('link filtered: {}'.format(link.url))
        return links

    def get_all_links(self, resp, regex):
        suble = LinkExtractor(allow=regex, unique=True, deny_extensions=None, strip=True)
        links = suble.extract_links(resp)
        str_links = []
        for link in links:
            str_links.append(link.url)
        return str_links

    def log_url(self):
        if grid_utils.is_row_present({'Job Posting Page': self.jp_link}, self.output_grid,
                                     grid_utils.qa_auth_id, 'qa'):
            return
        self.cp_row['Job Posting Page'] = self.jp_link
        self.insert_row["insert"]["rows"] = []
        self.insert_row["insert"]["rows"].append(self.cp_row)
        grid_utils.add_row(self.output_grid,
                           grid_utils.qa_auth_id,
                           self.insert_row)

    def is_contains_iframes(self):
        iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            return True
        return False

    def is_valid_job_posting(self, jp_url, rule=""):
        jp_keywords = [r"filter|refine|position|title|posted|engineer|openings|location", r"\bclear\b"]
        resp = self.get_scrapy_response(jp_url)
        if resp is not None:
            only_text = (html2text.html2text(resp.text)).lower()
            for jp_keyword in jp_keywords:
                if re.search(jp_keyword, only_text):
                    return True
        if self.is_contains_iframes():
            return True
        logging.info("validation failed : {}: {}".format(jp_url, rule))
        return False

    def is_valid_job_url(self, element, x_value_cnt, job_url_set):
        logging.info('is_valid_job_url check :: {}'.format(element.get_attribute("href")))
        if 'directions' in element.get_attribute("href"):
            return False
        if not (re.search(self.posting_regex_1, element.get_attribute("href"), re.IGNORECASE) or re.search(
                self.posting_regex_2, element.get_attribute("href"), re.IGNORECASE)):
            return False
        if element.location['x'] <= 0 or element.location['y'] <= 0:
            return False
        if element.get_attribute("href") in job_url_set:
            return False
        tokens = element.get_attribute("href").split('/')
        for token in tokens:
            if '-' in token or token.isnumeric():
                if element.location['x'] in x_value_cnt:
                    x_value_cnt[element.location['x']] = x_value_cnt.get(element.location['x']) + 1
                else:
                    x_value_cnt[element.location['x']] = 1
                job_url_set.add(element.get_attribute("href"))
                return True
        return False

    def is_job_titles_row_aligned(self, url):
        flag = False
        try:
            self.driver.get(url)
            x_value_cnt = {}
            job_url_set = set()
            time.sleep(2)
            elems = self.driver.find_elements(By.XPATH, "//a[@href]")
            time.sleep(1)
            logging.info('total number of hrefs: {}'.format(len(elems)))
            for elem in elems:
                if self.is_valid_job_url(elem, x_value_cnt, job_url_set):
                    logging.info('link:{} : location:{}'.format(elem.get_attribute("href"), elem.location))
            x_value_cnt_sorted = {k: v for k, v in sorted(x_value_cnt.items(), key=lambda item: item[1], reverse=True)}
            for key, val in x_value_cnt_sorted.items():
                if val >= 5:
                    logging.debug('key:{} : value:{}'.format(key, val))
                    flag = True
                break
        except:
            logging.error('is_job_titles_row_aligned {} : {}'.format(url, traceback.format_exc()))
        logging.info('is_job_titles_row_aligned: {} :{}'.format(url, flag))
        return flag

    def is_job_titles_in_frames(self, url):
        if url != self.driver.current_url:
            self.driver.get(url)
        wait = WebDriverWait(self.driver, 10)
        frames = wait.until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "iframe"))
        )
        flag = False
        try:
            for frame in frames:
                x_value_cnt = {}
                job_url_set = set()
                self.driver.switch_to.frame(frame)
                time.sleep(1)
                elements = self.driver.find_elements(By.XPATH, "//a[@href]")
                for element in elements:
                    if self.is_valid_job_url(element, x_value_cnt, job_url_set):
                        logging.info('link:{} : location:{}'.format(element.get_attribute("href"), element.location))
                x_value_cnt_sorted = {k: v for k, v in
                                      sorted(x_value_cnt.items(), key=lambda item: item[1], reverse=True)}
                for key, val in x_value_cnt_sorted.items():
                    if val >= 5:
                        logging.debug('key:{} : value:{}'.format(key, val))
                        flag = True
                    break
                self.driver.switch_to.parent_frame()
                time.sleep(1)
                if flag: break
        except:
            self.driver.switch_to.parent_frame()
            logging.error('no frames found {} : {}'.format(url, traceback.format_exc()))
        logging.info('is_job_titles_in_frames: {} :{}'.format(url, flag))
        return flag

    def is_job_titles_after_scroll(self, url):
        flag = False
        try:
            if url != self.driver.current_url:
                self.driver.get(url)
                time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(8)

            button_elements = self.driver.find_elements(By.XPATH, '//button')

            for button_element in button_elements:
                if re.search('description', button_element.text, re.IGNORECASE):
                    # print('found button job description on Page')
                    # print(button_element.text)
                    button_element.click()
                    break

            for window_handle in self.driver.window_handles:
                self.driver.switch_to.window(window_handle)
                body = self.driver.page_source
                html_response = HtmlResponse(url=self.driver.current_url, body=body, encoding='utf-8')
                html_text = html2text.html2text(html_response.text)
                if re.search(r"filter|refine|position|title|posted|engineer|openings|location",
                             self.job_meta_parser.clean_html(html_text),re.IGNORECASE):
                    flag = True
                    break
        except:
            logging.error('is_job_titles_after_scroll {} : {}'.format(url, traceback.format_exc()))
        logging.info('is_job_titles_after_scroll: {} :{}'.format(url, flag))
        return flag

    def is_job_titles_with_drop_down(self, url):
        flag = False
        try:
            if url != self.driver.current_url:
                self.driver.get(url)
                time.sleep(2)
            button_elements = self.driver.find_elements(By.XPATH, '//button')

            for button_element in button_elements:
                if re.search(keywords.JOB_TITLES, button_element.text, re.IGNORECASE):
                    # print('found button job title on Page')
                    # print(button_element.text)
                    flag = True
                    break
        except:
            logging.error('is_job_titles_with_drop_down {} : {}'.format(url, traceback.format_exc()))
        logging.info('is_job_titles_with_drop_down: {} :{}'.format(url, flag))
        return flag


    def rule_1(self):
        links = self.filter_links(self.posting_regex_1)
        # emitted = False
        rule_link = None
        for link in links:
            if not self.is_job_titles_row_aligned(link):
                continue
            if 'oraclecloud' in link:
                resp = self.get_scrapy_response(link)
                sub_links = self.get_all_links(resp, self.banking_regex)
                if len(sub_links) > 0:
                    rule_link = sub_links[0]
                    # emitted = True
                    break
            elif 0 < link.find('/job') < link.find('search'):
                rule_link = link
                # emitted = True
                break
            elif link.find('/job') > 0 and link.find('/jobs') < 0:
                rule_link = link
                # emitted = True
                break

        # if not emitted and len(links) > 0:
        #     rule_link = links[0]

        if rule_link is not None:
            if 'us/en' in self.driver.current_url:
                rule_link = rule_link.replace('us', 'us/en')
            # if not self.is_valid_job_posting(rule_link, "rule_1"):
            #     rule_link = None

        return rule_link

    def rule_2(self):
        links = self.filter_links(self.posting_regex_2)
        # rule_link = None
        for link in links:
            if self.is_job_titles_row_aligned(link):
                return link
        #     if self.is_valid_job_posting(link, "rule_2"):
        #         rule_link = link
        #         break
        #
        return None

    # def rule_3(self):
    #     return None

    def process(self):
        resp = self.get_scrapy_response(self.cp_row['Career Page'])
        if resp is not None:
            self.page_links = self.le.extract_links(resp)

            self.jp_link = self.rule_1()
            if self.jp_link is not None:
                logging.info("rule 1 passed:{}".format(self.jp_link))
                return

            self.jp_link = self.rule_2()
            if self.jp_link is not None:
                logging.info("rule 2 passed:{}".format(self.jp_link))
                return

            # self.jp_link = self.rule_3()
            # if self.jp_link is not None: return

    def add_to_uncrawled(self, error_reason):
        # logging.info("error::{}".format(error_reason))
        if not grid_utils.is_row_present({'career_page_url': self.cp_row['Career Page'],
                                          'Spider Name': self.spider_name},
                                         grid_utils.qa_uncrawl_grid_id, grid_utils.qa_auth_id, 'qa'):
            row = {'dmv_grid_name': self.cp_row['DMV Grid Name'],
                   'dmv_grid_id': self.cp_row['DMV Grid Id'],
                   'company_name': self.cp_row['Company Name'],
                   'company_url': self.cp_row['Company Website'],
                   'career_page_url': self.cp_row['Career Page'],
                   'Error Reason': error_reason,
                   'Spider Name': self.spider_name}
            self.uncrawl_dict["insert"]["rows"].append(row)
            grid_utils.add_row(grid_utils.qa_uncrawl_grid_id, grid_utils.qa_auth_id, self.uncrawl_dict)
            self.uncrawl_dict["insert"]["rows"] = []

    def run(self):
        logging.info('thread::{} started {}'.format(self.thread_id, self.cp_row['Career Page']))
        try:

            if self.is_job_titles_row_aligned(self.cp_row['Career Page']) and self.is_valid_job_posting(
                    self.cp_row['Career Page']):
                self.jp_link = self.cp_row['Career Page']
            elif self.is_job_titles_in_frames(self.cp_row['Career Page']) and self.is_valid_job_posting(
                    self.cp_row['Career Page']):
                self.jp_link = self.cp_row['Career Page']
            elif self.is_job_titles_after_scroll(self.cp_row['Career Page']):
                self.jp_link = self.cp_row['Career Page']
            elif self.is_job_titles_with_drop_down(self.cp_row['Career Page']):
                self.jp_link = self.cp_row['Career Page']
            else:
                self.process()
                if self.jp_link is not None and self.is_valid_job_posting(self.jp_link):
                    logging.debug("job posting page found by rules evaluation")
                else:
                    self.jp_link = None

            if self.jp_link is not None:
                logging.info(
                    'success:: career page: {} job posting page: {} thread_id: {}'.format(self.cp_row['Career Page'],
                                                                                          self.jp_link,
                                                                                          self.thread_id))
                self.log_url()
            else:
                logging.error("no job postings found:{}".format(self.cp_row['Career Page']))
                self.add_to_uncrawled('no job posting page found for {}'.format(self.cp_row['Career Page']))

        except Exception as e:
            logging.error("no job postings found:{}:{}".format(self.cp_row['Career Page']), traceback.format_exc())
            self.add_to_uncrawled(str(e))
            # traceback.print_exc()
        finally:
            self.driver.quit()
            logging.info('thread::{} ended {}'.format(self.thread_id, self.cp_row['Career Page']))


def get_debug_cps():
    debug_cps = [
        # "https://external-mythics.icims.com/jobs/search?ss=1&hashed=-435768531",
        # "https://hirequest.com/job-openings/"
        "https://intellibridge.us/join-our-team/career-opportunities/"
        # "http://www.madentech.com/pages/careers.html",
        # "http://www.tricorind.com/careers"
    ]
    cps = []
    for debug_cp in debug_cps:
        cps.append({'Career Page': debug_cp, "Debug": "debugging"})
    return cps


if __name__ == '__main__':
    try:
        start = time.time()
        thread_pool_size = 5
        target_grid = ''
        if len(sys.argv) > 1:
            thread_pool_size = int(sys.argv[1])
        else:
            print('please send thread pool size as param, example: '
                  'nohup python3 -u job_postings_extractor.py (thread_pool_size) (top_companies|dmv_companies|debug) > job_postings_extractor.log 2>&1 &')
            sys.exit(0)
        if 'top_companies' == sys.argv[2]:
            career_page_rows = grid_utils.get_grid_rows(grid_utils.qa_top_companies_grid)
            target_grid = grid_utils.qa_top_companies_jpp
        elif 'dmv_companies' == sys.argv[2]:
            career_page_rows = grid_utils.get_grid_rows(grid_utils.qa_career_grid_id)
            target_grid = grid_utils.qa_job_posting_page_grid
        elif 'debug' == sys.argv[2]:
            career_page_rows = get_debug_cps()
            target_grid = grid_utils.qa_top_companies_jpp
            for career_page_row in career_page_rows:
                t = JobPostingsExtractor(cp_row=career_page_row, output_grid=target_grid)
                t.start()

        logging.info('crawling {} career page links using {} threads'.format(len(career_page_rows), thread_pool_size))
        i = 0
        threads = []
        for career_page_row in career_page_rows:
            if grid_utils.is_row_present({'Career Page': career_page_row['Career Page']},
                                         target_grid, grid_utils.qa_auth_id, 'qa'):
                continue
            if i % thread_pool_size == 0:
                for t in threads:
                    t.join()
                threads = []
                i = 0
                time.sleep(5)
            t = JobPostingsExtractor(cp_row=career_page_row,output_grid=target_grid ,thread_id=i)
            t.start()
            threads.append(t)
            i = i + 1
        for t in threads:
            t.join()
        elapsed = time.time() - start
        logging.info('time elapsed in hours : {}'.format('{0:.2f}'.format(elapsed / (60 * 60))))
    except:
        traceback.print_exc()
