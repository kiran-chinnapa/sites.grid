import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

chrome_driver_path = os.getenv("chrome_driver_path")
s = Service(chrome_driver_path)
driver = webdriver.Chrome(service=s)


driver.get("https://hdpc.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/")

element = WebDriverWait(driver, 5).until(
    EC.presence_of_all_elements_located((By.TAG_NAME,"a"))
)
lists = driver.find_elements(By.TAG_NAME,"a")
for l in lists:
    print(l.get_attribute("href"))