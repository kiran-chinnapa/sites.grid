from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# url = 'https://google.com/search?q={} jobs in usa'.format('tcs')
# url = 'https://www.teksystems.com/careers'
url = 'https://www.experis.com/en/search'
chrome_options = Options()
chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36")
# chrome_options.headless = True
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
driver.get(url)
wait = WebDriverWait(driver, 10)
with open('/Users/msk/git_repos/sites.grid/src/main/resources/jobs.grid/simple.html','w') as f:
    f.write(driver.page_source)

# element = wait.until(
#     EC.presence_of_all_elements_located((By.CSS_SELECTOR,".iFjolb.gws-plugins-horizon-jobs__li-ed"))
# )
# if element is not None:
#     print("bingo got the google job tag")
# driver.quit()
