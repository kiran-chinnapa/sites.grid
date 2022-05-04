import os
import re
import requests
import time
import traceback
from requests_html import HTMLSession
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service


def get_driver(head_less = True):
    chrome_options = Options()
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.headless = head_less
    s = Service('/Users/msk/drivers/chromedriver')
    chrome_driver = webdriver.Chrome(service=s, options=chrome_options)
    return chrome_driver


def get_js_page_url(company_career_url):
    js_page_url = None
    try:
        chrome_options = Options()
        chrome_options.headless = True
        s = Service(os.getenv("chrome_driver_path"))
        driver = webdriver.Chrome(service=s, options=chrome_options)
        # driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        # driver = webdriver.Chrome(executable_path=os.getenv("chrome_driver_path"), options=chrome_options)
        driver.get(company_career_url)
        time.sleep(1)
        wait = WebDriverWait(driver, 10)
        title_textbox = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#keywordSearch"))
        )
        title_textbox[0].send_keys('Java Developer')
        time.sleep(1)
        location_textbox = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#gllocationInput"))
        )
        location_textbox[0].send_keys('USA')
        time.sleep(1)
        search_button = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#ph-search-backdrop"))
        )
        search_button[0].click()
        time.sleep(2)
        js_page_url = driver.current_url
        driver.quit()
    except:
        traceback.print_exc()
        driver.quit()
    return js_page_url


def get_job_links(company_website):
    tokens = company_website.split('.')
    _domain = '{}.{}'.format(tokens[len(tokens) - 2], tokens[len(tokens) - 1])
    links = [company_website]
    prefix_pattern = ['http://careers.{domain}', 'http://jobs.{domain}']
    for pat in prefix_pattern:
        links.append(pat.format(domain=_domain))
    suffix_keywords = ['/careers', '/jobs', '/careers/search', '/jobs/search']

    for key in suffix_keywords:
        links.append('{}{}'.format(company_website, key))
    return links


def get_job_tile_urls(company_website):
    chrome_agent = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'}
    # company_website = 'http://www.teksystems.com'
    # company_website = 'http://www.experis.com'
    # 3. Use the following patterns and look for job titles. careers.company.com , job.company.com , /careers, /jobs or /careers/search or /jobs/search
    keyword = 'job title'
    for job_pattern in get_job_links(company_website):
        # print('crawling pattern:{}'.format(job_pattern))
        try:
            resp = requests.get(job_pattern,
                                headers=chrome_agent, timeout=5)
            if resp.status_code in (200, 301, 302) and re.search(keyword, resp.text, re.IGNORECASE):
                return [job_pattern]
        except:
            # print('exception crawling: {}'.format(job_pattern))
            return []
    return []


# under construction
def scrape_js(js_page_url):
    print('scraping js page url:{}'.format(js_page_url))
    session = HTMLSession()
    # url = 'https://careers.teksystems.com/us/en/search-results?keywords=Java%20Developer&p=ChIJCzYy5IS16lQRQrfeQ5K5Oxw&location=USA'
    response = session.get(url)
    response.html.render(sleep=5, keep_page=True, scrolldown=1)
    for job_item in response.html.find('div.information > span > a'):
        print(job_item.links)
    session.close()

def urls_file():
    return  ['http://quotes.toscrape.com/']


if __name__ == '__main__':
    url = get_js_page_url("https://careers.teksystems.com/us/en")
    print('got url:{}'.format(url))
    scrape_js(url)

    # resp = requests.get('https://www.teksystems.com/careers',
    #                     headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'})
    # req = urllib.request.Request(
    #     'https://www.teksystems.com/careers',
    #     data=None,
    #     headers={
    #         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
    #     }
    # )
    # f = urllib.request.urlopen(req)
    # # print(f.read().decode('utf-8'))
    # with open('response.html', 'w') as file:
    #     file.write(f.read().decode('utf-8'))
