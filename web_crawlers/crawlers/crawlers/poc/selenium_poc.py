import os
import time
import traceback
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service


def print_js_page(company_career_url):
    js_page_url = None
    try:
        chrome_driver_path = os.getenv("chrome_driver_path")
        if chrome_driver_path is None:
            print('please set environment variable chrome_driver_path')
            return
        chrome_options = Options()
        # chrome_options.headless = True
        s = Service(chrome_driver_path)
        driver = webdriver.Chrome(service=s, options=chrome_options)
        # driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        # driver = webdriver.Chrome(executable_path=os.getenv("chrome_driver_path"), options=chrome_options)
        driver.get(company_career_url)
        time.sleep(5)
        html = driver.page_source
        time.sleep(2)
        print(html)

        # time.sleep(1)
        # wait = WebDriverWait(driver, 10)
        # title_textbox = wait.until(
        #     # EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#keywordSearch"))
        #     EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[placeholder=\"Search by Job Title, Keywords\"]"))
        # )
        # title_textbox[0].send_keys('Java Developer')
        # time.sleep(1)
        # location_textbox = wait.until(
        #     # EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#gllocationInput"))
        #     EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[placeholder=\"Enter City, State or Zip\"]"))
        # )
        # location_textbox[0].send_keys('USA')
        # time.sleep(1)
        # search_button = wait.until(
        #     # EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#ph-search-backdrop"))
        #     EC.presence_of_all_elements_located((By.XPATH, "//button[contains(.,'Search')]"))
        # )
        # search_button[0].click()
        # time.sleep(2)
        # js_page_url = driver.current_url
        driver.quit()
    except:
        traceback.print_exc()
        driver.quit()
    return js_page_url


def get_js_page_url(company_career_url):
    js_page_url = None
    try:
        chrome_driver_path = os.getenv("chrome_driver_path")
        if chrome_driver_path is None:
            print('please set environment variable chrome_driver_path')
            return
        chrome_options = Options()
        chrome_options.headless = True
        s = Service(chrome_driver_path)
        driver = webdriver.Chrome(service=s, options=chrome_options)
        # driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        # driver = webdriver.Chrome(executable_path=os.getenv("chrome_driver_path"), options=chrome_options)
        driver.get(company_career_url)
        time.sleep(1)
        wait = WebDriverWait(driver, 10)
        title_textbox = wait.until(
            # EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#keywordSearch"))
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[placeholder=\"Search by Job Title, Keywords\"]"))
        )
        title_textbox[0].send_keys('Java Developer')
        time.sleep(1)
        location_textbox = wait.until(
            # EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#gllocationInput"))
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[placeholder=\"Enter City, State or Zip\"]"))
        )
        location_textbox[0].send_keys('USA')
        time.sleep(1)
        search_button = wait.until(
            # EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#ph-search-backdrop"))
            EC.presence_of_all_elements_located((By.XPATH, "//button[contains(.,'Search')]"))
        )
        search_button[0].click()
        time.sleep(2)
        js_page_url = driver.current_url
        driver.quit()
    except:
        traceback.print_exc()
        driver.quit()
    return js_page_url


if __name__ == '__main__':
    top_company_career_pages = [
        'https://www.accenture.com/careers.html',
        'https://careers.bankofamerica.com/en-us',
        'https://careers.jpmorgan.com/US/en/home',
        'https://www.goldmansachs.com/careers/index.html',
        'https://www.facebookcareers.com',
        'https://careers.microsoft.com/us/en',
        'https://www.apple.com/careers/us/',
        'https://www.amazon.jobs/en/'
    ]
    # url = 'https://careers.teksystems.com/'
    # url = 'https://www.experis.com/en/careers'
    # js_page = get_js_page_url(top_company_career_pages[0])
    # print(js_page)
    print_js_page('https://careers-obxtek.icims.com/jobs/intro?hashed=-435683781')


def get_js_page_url_old():
    url = 'https://careers.teksystems.com/us/en'
    try:
        chrome_options = Options()
        # chrome_options.headless = True
        s = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=s, options=chrome_options)
        # driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        driver.get('https://careers.teksystems.com/us/en')
        time.sleep(1)
        wait = WebDriverWait(driver, 10)
        title_textbox = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[placeholder=\"Search by Job Title, Keywords\"]"))
        )
        title_textbox[0].send_keys('Java Developer')
        time.sleep(1)
        location_textbox = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "[placeholder=\"Enter City, State or Zip\"]"))
        )
        location_textbox[0].send_keys('USA')
        time.sleep(1)
        search_button = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "button:contains(\"Search\")"))
        )
        search_button[0].click()
        time.sleep(2)
        print(driver.current_url)
        # search_buttons[0].click()
        # time.sleep(10)
        # self.sleep(2)
        # self.click('#keywordSearch')
        # self.send_keys('#keywordSearch', 'Java Developer')
        # self.sleep(1)
        # self.click('#gllocationInput')
        # self.send_keys('#gllocationInput', 'USA')
        # # click the search button
        # self.sleep(5)
        # self.click("#ph-search-backdrop")
        # # verify search results
        # self.sleep(5)
        # print('**********************')
        # url = self.driver.current_url
        # self.scrape_js(url)
    except:
        traceback.print_exc()
        driver.quit()
    driver.quit()
