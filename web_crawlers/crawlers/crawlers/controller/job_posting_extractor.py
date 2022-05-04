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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import html2text

posting_regex = r"/search-results|/jobs|/job-search|/search|/job_categories/|/jobsearch|.oraclecloud."
banking_regex = r"/requisitions"

# def process_value(value):
#     return value.lower()


def get_scrapy_response(url):
    try:
        driver.get(url)
        time.sleep(5)
        body = driver.page_source
        return HtmlResponse(url=driver.current_url, body=body, encoding='utf-8')
    except Exception as e:
        print('unable to process url :{} got exception :{}'.format(url, str(e)))


def get_all_links(response, regex):
    le = LinkExtractor(
        allow=regex,
        unique=True,
        # process_value=process_value,
        deny_extensions=None,
        strip=True)
    links = le.extract_links(response)
    str_links = []
    for link in links:
        str_links.append(link.url)
        # print(link.url)
    return str_links


# def search_button_approach(target_posting_page):
#     for button_name in button_names:
#         search_button = None
#         try:
#             wait = WebDriverWait(driver, 5)
#             search_button = wait.until(
#                 EC.presence_of_all_elements_located((By.XPATH, "//button[contains(.,'{}')]".format(button_name)))
#             )
#         except:
#             print('button named "{}" not found'.format(button_name))
#         if search_button is not None:
#             search_button[0].click()
#             time.sleep(2)
#             if target_posting_page in driver.current_url:
#                 print("bingo got the page:{} by search_button_approach".format(driver.current_url))
#                 return True
#     return False


# def get_all_buttons():
#     try:
#         wait = WebDriverWait(driver, 5)
#         buttons = wait.until(
#             EC.presence_of_all_elements_located((By.TAG_NAME, "button"))
#         )
#         for button in buttons:
#             print(button)
#     except:
#         pass


# def sub_links_approach(target_posting_page):
#     page_links = get_all_links(resp)
#     for page_link in page_links:
#         # print(page_link)
#         target_tokens = target_posting_page.split('/')
#         tokens = page_link.split('/')
#         if tokens[len(tokens) - 1] == target_tokens[len(target_tokens) -1]:
#             print("bingo got the page:{} by sub_links_approach".format(page_link))
#             return True
#         if target_posting_page in page_link:
#             print("bingo got the page:{} by sub_links_approach".format(page_link))
#             return True
#         if 'oraclecloud' in page_link or 'search' in page_link:
#             if 'requisitions' in page_link:
#                 print("bingo got the page:{} by sub_links_approach".format(page_link))
#                 return True
#             if page_link in target_posting_page:
#                 print("bingo got the page:{} by sub_links_approach".format(page_link))
#                 return True
#             sub_tokens = page_link.split('/')
#             if sub_tokens[len(sub_tokens)-1] in target_posting_page:
#                 print("bingo got the page:{} by sub_links_approach".format(page_link))
#                 return True
#             sub_resp = get_scrapy_response(page_link)
#             sub_page_links = get_all_links(sub_resp)
#             for sub_page_link in sub_page_links:
#                 if 'requisitions' in sub_page_link:
#                     print("bingo got the page:{} by sub_sub_links_approach".format(sub_page_link))
#                     return True
#     return False

def is_valid_job_posting(jp_url):
    keywords = [r"filter|refine|position|title|posted", r"\bclear\b"]
    resp = get_scrapy_response(jp_url)
    only_text = (html2text.html2text(resp.text)).lower()
    for keyword in keywords:
        if re.search(keyword, only_text):
            return True
    print('invalid:{}'.format(jp_url))
    return False



career_page_urls = [
    # "https://careers.microsoft.com/",
    # "https://careers.jpmorgan.com",
    # "https://www.goldmansachs.com/careers/index.html",
    # "https://www.facebook.com/careers/?ref=pf",
    # "http://careers.bankofamerica.com/",
    # "https://www.apple.com/careers/us/",
    # "https://www.amazon.jobs",
    # "https://www.accenture.com/us-en/careers"
    # "https://guidehouse.com/careers",
    # 'https://educegroup.com/company/careers/',
    # 'http://www.ctsinternational.com/jobs.aspx?x=all',
    'http://www.ustas.us/jobseekers_benefits.php'
]

target_posting_pages = [
    'https://careers.microsoft.com/us/en/search-results',
    'https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions',
    'https://hdpc.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions?sortBy=POSTING_DATES_DESC',
    'https://www.facebookcareers.com/jobs',
    'https://careers.bankofamerica.com/en-us/job-search?ref=search&start=0&rows=10&search=getAllJobs',
    'https://jobs.apple.com/en-us/search?location=united-states-USA',
    'https://www.amazon.jobs/en/job_categories/software-development',
    'https://www.accenture.com/us-en/careers/jobsearch?jk=&sb=1&pg=1&is_rj=0'

]



# button_names = ["Find jobs"]

try:
    chrome_driver_path = os.getenv("chrome_driver_path")
    if chrome_driver_path is None:
        print('please set environment variable chrome_driver_path to /Users/msk/drivers/chromedriver on mac')
        sys.exit(0)
    chrome_options = Options()
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.headless = True
    s = Service(chrome_driver_path)
    final_link = ""
    for i in range(len(career_page_urls)):
        driver = webdriver.Chrome(service=s, options=chrome_options)
        print('crawling {}'.format(career_page_urls[i]))
        resp = get_scrapy_response(career_page_urls[i])
        if resp is not None:
            links = get_all_links(resp, posting_regex)
            emitted = False
            for link in links:
                if 'oraclecloud' in link:
                    resp = get_scrapy_response(link)
                    sub_links = get_all_links(resp, banking_regex)
                    if len(sub_links) > 0:
                        final_link = sub_links[0]
                        emitted = True
                        break
                elif 0 < link.find('/job') < link.find('search'):
                    final_link = link
                    emitted = True
                    break
                elif link.find('/job') > 0 and link.find('/jobs') < 0:
                    final_link = link
                    emitted = True
                    break
            if not emitted and len(links) > 0:
                final_link = links[0]
            # if sub_links_approach(target_posting_pages[i]):
            #     continue
            # if search_button_approach(target_posting_pages[i]):
            #     continue
        if 'us/en' in driver.current_url:
            final_link = final_link.replace('us', 'us/en')
        if is_valid_job_posting(final_link):
            print(final_link)
        elif is_valid_job_posting(career_page_urls[i]):
            print(career_page_urls[i])

    driver.quit()
except Exception as e:
    print('exception details {}'.format(str(e)))
    traceback.print_exc()

driver.quit()


