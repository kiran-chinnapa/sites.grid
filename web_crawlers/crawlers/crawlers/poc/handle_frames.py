import time
from crawlers.utils import crawl_utils
from scrapy.http import HtmlResponse
from scrapy.linkextractors import LinkExtractor
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By


def get_links_from_frames(url, driver):
    driver.get(url)

    wait = WebDriverWait(driver, 10)
    frames = wait.until(
        EC.presence_of_all_elements_located((By.TAG_NAME, "iframe"))
    )
    for frame in frames:
        try:
            driver.switch_to.frame(frame)
            time.sleep(1)
            # body = driver.page_source
            # html_response = HtmlResponse(url=driver.current_url, body=body, encoding='utf-8')
            # link_extractor = LinkExtractor(unique=True, deny_extensions=None, strip=True)
            # # links = link_extractor.extract_links(html_response)
            # # for link in links:
            # #     print(link.url)
            elements = driver.find_elements(By.XPATH, "//a[@href]")
            for element in elements:
                print(element.get_attribute("href"))
            driver.switch_to.parent_frame()
            time.sleep(1)
        except:
            driver.switch_to.parent_frame()
            print('no frames found')


if __name__ == '__main__':
    driver = crawl_utils.get_driver()
    get_links_from_frames('https://invex-careers.s3.amazonaws.com/index.html', driver)
    driver.quit()
