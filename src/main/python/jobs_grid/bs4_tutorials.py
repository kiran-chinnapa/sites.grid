from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
source = requests.get('https://www.google.com/search?q=tcs%20jobs%20in%20usa',headers=headers).text
# chrome_options = Options()
# chrome_options.headless = True
# s = Service(ChromeDriverManager().install())
# driver = webdriver.Chrome(service=s, options=chrome_options)
# driver.get('https://www.google.com/search?q=tcs%20jobs%20in%20usa')
# print(driver.page_source)
# with open('../../resources/jobs_grid/simple.html') as file:
#     source = file.read()
soup = BeautifulSoup(source, 'lxml')
# print(soup.prettify())
g_card = soup.find('g-card')
if g_card is not None:
    print(g_card.prettify())
else:
    print(soup.title.text)


