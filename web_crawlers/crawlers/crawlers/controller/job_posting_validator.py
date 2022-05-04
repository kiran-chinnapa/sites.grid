import logging
import re
import sys
import time
import traceback
import html2text
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from selenium.webdriver.common.by import By
from crawlers.utils import crawl_utils, grid_utils
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


def is_contains_iframes():
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    if iframes:
        return True
    return False
    # for iframe in iframes:
    #     driver.switch_to.frame(iframe)
    #     time.sleep(5)
    #     print(driver.page_source)
    #     print("*************************")
    # sys.exit()


def get_scrapy_response(url):
    try:
        driver.get(url)
        time.sleep(5)
        body = driver.page_source
        return HtmlResponse(url=driver.current_url, body=body, encoding='utf-8')
    except Exception as e:
        print('unable to process url :{} got exception :{}'.format(url, str(e)))


def is_valid_job_posting(jp_url):
    jp_keywords = [r"filter|refine|position|title|posted|engineer|openings", r"\bclear\b"]
    resp = get_scrapy_response(jp_url)
    if resp is not None:
        only_text = (html2text.html2text(resp.text)).lower()
        for jp_keyword in jp_keywords:
            if re.search(jp_keyword, only_text):
                return True
    if is_contains_iframes():
        return True
    print("validation failed : {}: {}".format(jp_url, 0))
    return False


def is_valid_job_posting_page(url):
    try:
        x_value_cnt = {}
        job_url_set = set()
        driver.get(url)
        time.sleep(5)
        elems = driver.find_elements(By.XPATH, "//a[@href]")
        print('hrefs size:{}'.format(len(elems)))
        time.sleep(1)
        for elem in elems:
            if is_valid_job_url(elem, x_value_cnt, job_url_set):
                logging.info('link:{} : location:{}'.format(elem.get_attribute("href"), elem.location))
        x_value_cnt_sorted = {k: v for k, v in sorted(x_value_cnt.items(), key=lambda item: item[1], reverse=True)}
        for key, val in x_value_cnt_sorted.items():
            if val >= 10:
                logging.info('key:{} : value:{}'.format(key, val))
                return True
            break
        return False
    except:
        logging.error('error with url {}'.format(url))
        return False


def get_all_links(resp):
    suble = LinkExtractor(unique=True, deny_extensions=None, strip=True)
    links = suble.extract_links(resp)
    str_links = []
    for link in links:
        if is_valid_job_url(link.url):
            str_links.append(link.url)
    return str_links


def is_valid_job_url(element, x_value_cnt, job_url_set):
    posting_regex_1 = r"/search-results|/jobs|/job-search|/search|/job_categories/|/jobsearch|.oraclecloud.|joblistpage|posting|apply"
    posting_regex_2 = r"job|career|recruitment|openings"
    logging.info('evaluating alignment of :: {}'.format(element.get_attribute("href")))
    if 'Posting' in element.get_attribute("href"):
        print('debug')
    if not(re.search(posting_regex_1, element.get_attribute("href"), re.IGNORECASE) or re.search(
            posting_regex_2, element.get_attribute("href"), re.IGNORECASE)):
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


if __name__ == '__main__':
    driver = crawl_utils.get_driver()
    driver.implicitly_wait(10)
    # with open('../resources/Job_Posting_Page_grid.csv') as f:
    #     reader = csv.DictReader(f, delimiter=",")
    #     for row in reader:
    #         flag = "invalid"
    #         if is_valid_job_posting(row['Job Posting Page']):
    #             flag = "valid"
    #         print('{} :: {}'.format(row['Job Posting Page'], flag))
    # print(is_job_links_present("https://www.verizon.com/about/careers/"))
    target_posting_pages = [
        # 'http://15e.34e.myftpupload.com/careers/',
        # 'http://1stchoicegov.com/careerportal/#/jobs',
        # 'http://99999consulting.com/careers/#nav',
        # 'http://amaram.com/more-it-workers-come-to-the-us-more-it-jobs-will-stay-here/#respond',
        # 'http://amaram.com/opportunities/open-positions/',
        'https://jobs.systemoneservices.com/all-jobs'
    ]
    print(is_valid_job_posting_page(target_posting_pages[0]))
    driver.quit()
    sys.exit(0)
    job_posting_rows = grid_utils.get_grid_rows(grid_utils.qa_job_posting_page_grid)
    jp_cp_same_cnt = 0
    unique_jp_cnt = 0
    print('link location job posting validator started')
    for job_posting_row in job_posting_rows:
        if not is_valid_job_posting_page(job_posting_row['Job Posting Page']):
            if job_posting_row['Job Posting Page'] == job_posting_row['Career Page']:
                jp_cp_same_cnt = jp_cp_same_cnt + 1
                print('same as career page: {}'.format(job_posting_row['Job Posting Page']))
            else:
                unique_jp_cnt = unique_jp_cnt + 1
                print('unique jp: {}'.format(job_posting_row['Job Posting Page']))
    print('total invalid job posting pages: same as career page: {}'.format(jp_cp_same_cnt))
    print('total invalid job posting pages: unique jp cnt: {}'.format(unique_jp_cnt))
    # print(is_contains_iframes())
    driver.quit()
