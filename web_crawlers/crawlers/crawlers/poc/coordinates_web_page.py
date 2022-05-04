import time

from selenium.webdriver.common.by import By
from crawlers.utils import crawl_utils

driver = crawl_utils.get_driver()

driver.get('https://www.yatra.com/')
time.sleep(1)
element = driver.find_element(By.LINK_TEXT,"Yatra for Business")
time.sleep(1)

print(element.location)
print(element.size)
print(element.size['width'], element.size['height'])

time.sleep(5)
driver.quit()