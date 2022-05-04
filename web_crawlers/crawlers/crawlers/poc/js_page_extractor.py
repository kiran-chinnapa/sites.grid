import os
import time
import traceback

from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

def print_href():
    wait = WebDriverWait(driver, 10)
    hrefs = wait.until(
        EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
    )
    # hrefs = driver.find_elements(By.TAG_NAME, "a")
    for href in hrefs:
        print(href.get_attribute('href'))

job_page_url = 'http://www.1901group.com/careers-jobs/'
chrome_driver_path = os.getenv("chrome_driver_path")
chrome_options = Options()
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
chrome_options.add_argument(f'user-agent={user_agent}')
chrome_options.headless = True
s = Service(chrome_driver_path)
driver = webdriver.Chrome(service=s, options=chrome_options)
time.sleep(5)
driver.get(job_page_url)
print_href()

wait = WebDriverWait(driver, 10)
frames = wait.until(
        EC.presence_of_all_elements_located((By.TAG_NAME, "iframe"))
    )
# frames = driver.find_elements(By.TAG_NAME,"iframe")
for frame in frames:
    # frame_id = frame.get_attribute("id")
    try:
        driver.switch_to.frame(frame)
        print_href()
        driver.switch_to.parent_frame()
    except:
        driver.switch_to.parent_frame()
        print('no hrefs found')







# print(driver.page_source)