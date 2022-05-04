import re
import time
from crawlers.utils import crawl_utils
from scrapy.http import HtmlResponse
from selenium.webdriver.common.by import By
import html2text


def clean_html(html_text):
    html_text = html_text.replace('\n\n', '\n').replace('#', ' ').replace('*', ' ')
    clean = []
    for line in html_text.split('\n'):
        if '[' not in line or ']' not in line:
            clean.append(line)
    return '\n'.join(clean)


driver = crawl_utils.get_driver()
driver.get('https://hirequest.com/job-openings/')
time.sleep(2)
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(8)

button_elements = driver.find_elements(By.XPATH, '//button')

for button_element in button_elements:
    if re.search('description', button_element.text, re.IGNORECASE):
        print('found button job description on Page')
        # print(button_element.text)
        button_element.click()
        break

for window_handle in driver.window_handles:
    driver.switch_to.window(window_handle)
    body = driver.page_source
    html_response = HtmlResponse(url=driver.current_url, body=body, encoding='utf-8')
    html_text = html2text.html2text(html_response.text)
    print(clean_html(html_text))

driver.quit()
