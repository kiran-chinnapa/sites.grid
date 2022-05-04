import os
import sys
import threading
import time
import traceback
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from crawlers.controller.page_navigator import PageNavigator
from crawlers.utils import grid_utils
import logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


class JobExtractorEngine(threading.Thread):

    def __init__(self, jp_row=None, thread_id=0):
        self.spider_name = 'job-extractor-engine'
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
        self.jp_row = jp_row
        self.insert_row = {"insert": {"rows": []}}
        self.uncrawl_dict = {"insert": {"rows": []}}
        self.page_navigator = PageNavigator(self.driver, self.jp_row)
        threading.Thread.__init__(self)

    def terminate(self):
        self.page_navigator.running = False

    def add_to_uncrawled(self, error_reason):
        logging.error(error_reason)
        if not grid_utils.is_row_present({'job_posting_page_url': self.jp_row['Job Posting Page'],
                                          'career_page_url': self.jp_row['Career Page'],
                                          'Spider Name': self.spider_name},
                                         grid_utils.qa_uncrawl_grid_id,grid_utils.qa_auth_id, 'qa'):
            row = {'dmv_grid_name': self.jp_row['DMV Grid Name'],
                   'dmv_grid_id': self.jp_row['DMV Grid Id'],
                   'company_name': self.jp_row['Company Name'],
                   'company_name': self.jp_row['Company Name'],
                   'company_url': self.jp_row['Company Website'],
                   'career_page_url': self.jp_row['Career Page'],
                   'Error Reason': error_reason,
                   'Spider Name': self.spider_name,
                   'job_posting_page_url': self.jp_row['Job Posting Page']}
            self.uncrawl_dict["insert"]["rows"].append(row)
            grid_utils.add_row(grid_utils.qa_uncrawl_grid_id, grid_utils.qa_auth_id, self.uncrawl_dict)
            self.uncrawl_dict["insert"]["rows"] = []

    def run(self):
        start_time = time.time()
        logging.info('thread::{} started {}'.format(threading.get_ident(), self.jp_row['Job Posting Page']))
        try:
            self.page_navigator.process()
        except Exception as e:
            logging.error("exception in thread: {} \n{}".format(self.jp_row['Job Posting Page'], traceback.format_exc()))
            self.add_to_uncrawled(str(e))
        finally:
            self.driver.quit()
            # logging.info('thread::{} ended {}'.format(self.thread_id, self.jp_row['Job Posting Page']))
            logging.info('thread::{} completed {} time_elapsed:{}'.format(threading.get_ident(), self.jp_row['Job Posting Page'], round(time.time() - start_time,2)))


def get_debug_job_posting_pages():
    debug_cps = [
        # "https://angel.co/1787fp/jobs",
        # "https://www.1901group.com/careers/",
        # "https://opensourceconnections.com/about-us/careers/",
        # "http://2hb.com/html/careers.html",
        # "https://www.cdwjobs.com/search/data-security/jobs?per_page=10&search=Search".
        "https://invex-careers.s3.amazonaws.com/index.html/"
    ]
    cps = []
    for debug_cp in debug_cps:
        cps.append({'Job Posting Page': debug_cp, "Debug": "debugging"})
    return cps


if __name__ == '__main__':
    try:
        start = time.time()
        thread_pool_size = 5
        if len(sys.argv) > 1:
            thread_pool_size = int(sys.argv[1])
        else:
            print('please send thread pool size as param, example: '
                  'nohup python3 -u job_extractor_engine.py (thread_pool_size) (top_companies|dmv_companies|debug) > job_extractor_engine.log 2>&1 &')
            sys.exit(0)
        if 'top_companies' == sys.argv[2]:
            job_posting_rows = grid_utils.get_grid_rows(grid_utils.qa_top_companies_jpp)
        elif 'dmv_companies' == sys.argv[2]:
            job_posting_rows = grid_utils.get_grid_rows(grid_utils.qa_job_posting_page_grid)
        elif 'debug' == sys.argv[2]:
            job_posting_rows = get_debug_job_posting_pages()
            for job_posting_row in job_posting_rows:
                t = JobExtractorEngine(jp_row=job_posting_row)
                t.start()

        logging.info('crawling {} job posting page links using {} threads'.format(len(job_posting_rows), thread_pool_size))
        i = 0
        threads = []
        max_seconds = 21600
        check_time = 600
        for job_posting_row in job_posting_rows:
            if grid_utils.is_row_present({'Job Posting Page': job_posting_row['Job Posting Page']},
                                         grid_utils.latest_jobs_grid_id, grid_utils.qa_auth_id, 'qa'):
                continue
            if i % thread_pool_size == 0:
                for t in threads:
                    start = time.time()
                    while t.is_alive():
                        logging.info('thread : {} is alive'.format(t.ident))
                        time.sleep(check_time)
                        if (time.time() - start) > max_seconds:
                            logging.info('killing thread : {} : running more than : {} : seconds'.format(t.ident, max_seconds))
                            t.terminate()
                            break
                    t.join()
                    logging.info('thread : {} is alive : {}'.format(t.ident, t.is_alive()))
                threads = []
                i = 0
                time.sleep(5)
            t = JobExtractorEngine(jp_row=job_posting_row, thread_id=i)
            t.start()
            threads.append(t)
            i = i + 1
        for t in threads:
            t.join()
        elapsed = time.time() - start
        logging.info('time elapsed in hours : {}'.format('{0:.2f}'.format(elapsed / (60 * 60))))
    except:
        logging.error("exception in main thread: \n{}".format(traceback.format_exc()))
