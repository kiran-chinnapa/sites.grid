import os
import sys
import threading
import time
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from crawlers.utils import keywords
from crawlers.utils import grid_utils


class JSLinkExtnThread(threading.Thread):

    def __init__(self, url_meta_data, thread_id=0):
        self.url_meta_data = url_meta_data
        self.le = LinkExtractor(allow=keywords.CAREER_PAGE, unique=True, process_value=self.process_value,
                                deny_extensions=None,
                                strip=True)
        chrome_driver_path = os.getenv("chrome_driver_path")
        if chrome_driver_path is None:
            print('please set environment variable chrome_driver_path to /Users/msk/drivers/chromedriver on mac')
            sys.exit(0)
        chrome_options = Options()
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.headless = True
        s = Service(chrome_driver_path)
        self.driver = webdriver.Chrome(service=s, options=chrome_options)
        self.thread_id = thread_id
        self.js_career_pg_ctr = 0
        threading.Thread.__init__(self)

    def process_value(self, value):
        return value.lower()

    def get_scrapy_response(self, url):
        try:
            self.driver.get(url)
            body = self.driver.page_source
            return HtmlResponse(url=self.driver.current_url, body=body, encoding='utf-8')
        except Exception as e:
            print('unable to process url :{} got exception :{}'.format(url, str(e)))

    def get_all_links(self, response):
        links = self.le.extract_links(response)
        str_links = []
        for link in links:
            str_links.append(link.url)
        return str_links

    def run(self):
        try:
            resp = self.get_scrapy_response(self.url_meta_data['company_url'])
            if resp is not None:
                page_links = self.get_all_links(resp)
                if len(page_links) > 0:
                    print('thread {} processing company {} pages-links count:{}'.format(self.thread_id, self.url_meta_data['company_url'], len(page_links)))
                else:
                    print('thread {} processing company {} got zero pages-links'.format(self.thread_id,self.url_meta_data['company_url']))
            else:
                print('thread {} processing company {} got zero pages-links'.format(self.thread_id,self.url_meta_data['company_url']))
        except Exception as e:
            print('exception in run() {}'.format(str(e)))
        finally:
            self.driver.quit()


if __name__ == '__main__':
    start = time.time()
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level=logging.ERROR,
    #     datefmt='%Y-%m-%d %H:%M:%S')
    # configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
    # js_thread = JSLinkExtnThread('https://www.cogniertechnology.com/', driver)
    # js_thread.start()
    thread_pool_size = 5
    if len(sys.argv) > 1:
        thread_pool_size = int(sys.argv[1])
    else:
        print('please send thread pool size as param, example: '
              'python3 js_url_extraction_spider.py 10 > js_url_extraction_spider.log 2>&1 &')
        sys.exit(0)
    uncrawled_comps = grid_utils.get_uncrawled_companies()
    print('total uncrawled links are: {} going to run in {} threads'.format(len(uncrawled_comps), thread_pool_size))
    # uncrawled_comps = [{'company_url':'https://www.cogniertechnology.com/'},
    #                    {'company_url':'https://www.teksystems.com/'},
    #                    {'company_url':'https://www.experis.com/'},
    #                    {'company_url':'https://www.amazon.com/'},
    #                    {'company_url':'https://www.google.com/'}
    #                    ]
    i = 0
    threads = []
    for uncrawled_comp in uncrawled_comps:
        # if str(st_row) in lst:
        #     continue
        if i % thread_pool_size == 0:
            for t in threads:
                t.join()
            threads = []
            time.sleep(5)
        t = JSLinkExtnThread(url_meta_data=uncrawled_comp, thread_id=i)
        t.start()
        threads.append(t)
        i = i + 1
    for t in threads:
        t.join()
    elapsed = time.time() - start
    print('time elapsed in seconds : {}'.format(elapsed))
