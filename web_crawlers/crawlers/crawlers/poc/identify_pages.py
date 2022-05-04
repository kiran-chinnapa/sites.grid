import re
import time
from scrapy.http import HtmlResponse
from crawlers.utils import crawl_utils


def get_scrapy_response(url):
    try:
        if driver.current_url != url:
            driver.get(url)
            time.sleep(5)
        body = driver.page_source
        return HtmlResponse(url=driver.current_url, body=body, encoding='utf-8')
    except Exception as e:
        raise Exception('chrome_driver unable to process url :{} exception :{}'.format(url, str(e)))


driver = crawl_utils.get_driver()
keyword = "pagination"
url1 = "https://www.amazon.jobs/en/job_categories/software-development?offset=10&result_limit=10&sort=relevant&distanceType=Mi&radius=24km&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&"
url2 = "https://hdpc.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions?sortBy=POSTING_DATES_DESC"

resp = get_scrapy_response(url2)
# print(resp.text)
if re.search('pagination', resp.text, re.IGNORECASE):
    print('has pages')
elif re.search('scroll', resp.text, re.IGNORECASE):
    print('has scrolling with show more or load more pages')
else:
    print('has single page')