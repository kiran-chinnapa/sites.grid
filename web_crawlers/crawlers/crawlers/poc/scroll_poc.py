import time
from selenium.webdriver.common.keys import Keys
from crawlers.utils import crawl_utils
from selenium.webdriver.common.by import By

SCROLL_PAUSE_TIME = 10
url = 'https://careers.bankofamerica.com/en-us/job-search?ref=search&start=0&rows=10&search=getAllJobs'
driver = crawl_utils.get_driver(False)
driver.get(url)
time.sleep(5)
# Get scroll height
SCROLL_PAUSE_TIME = 5
#
# # Get scroll height
# last_height = driver.execute_script("return document.body.scrollHeight")
#
# while True:
    # Scroll down to bottom
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(SCROLL_PAUSE_TIME)
driver.execute_script("window.scrollTo(document.body.scrollHeight,500)")
time.sleep(SCROLL_PAUSE_TIME)
driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
time.sleep(SCROLL_PAUSE_TIME)
driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
time.sleep(SCROLL_PAUSE_TIME)
    # Wait to load page
    # time.sleep(SCROLL_PAUSE_TIME)
    #
    # # Calculate new scroll height and compare with last scroll height
    # new_height = driver.execute_script("return document.body.scrollHeight")
    # if new_height == last_height:
    #     break
    # last_height = new_height
elements = driver.find_elements(By.CSS_SELECTOR, ".search-results-load-more-btn")
print(len(elements))


driver.quit()