import os
import re
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from crawlers.utils import keywords

'''recursive crawl until apply now'''


def contains_job_title(link):
    page_source = driver.page_source
    if re.search("apply now", page_source, re.IGNORECASE):
        return True
    return False


def get_sub_links(url):
    links = set()
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    hrefs = wait.until(
        EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
    )
    # hrefs = driver.find_elements(By.TAG_NAME, "a")
    for href in hrefs:
        links.add(href.get_attribute('href'))
    return links


chrome_driver_path = os.getenv("chrome_driver_path")
chrome_options = Options()
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
chrome_options.add_argument(f'user-agent={user_agent}')
chrome_options.headless = True
s = Service(chrome_driver_path)
driver = webdriver.Chrome(service=s, options=chrome_options)
job_page_url = 'https://careers.teksystems.com/us/en'

sub_links = get_sub_links(job_page_url)
for sub_link in sub_links:
    if contains_job_title(sub_link):
        print(sub_link)
        break
