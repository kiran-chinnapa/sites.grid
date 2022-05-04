import concurrent.futures
import re
import requests
from crawlers.utils import crawl_utils, keywords

chrome_agent = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'}


def handle_request(job_pattern):
    # print('hello {}'.format(bar))
    try:
        resp = requests.get(job_pattern,
                            headers=chrome_agent, timeout=5)
        if resp.status_code in (200, 301, 302) and re.search(keywords.JOB_TITLE, resp.text, re.IGNORECASE):
        # if resp.status_code in (200, 301, 302):
            return job_pattern
    except:
        return None
    return None


def process(company_website):
    job_urls =[]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # future = executor.submit(foo, 'world!')
        # return_value = future.result()
        # print(return_value)
        param_list = crawl_utils.get_job_links(company_website)
        futures = [executor.submit(handle_request, param) for param in param_list]
        # [print(f.result()) for f in futures]
        for f in futures:
            if f.result() is not None: job_urls.append(f.result())
        return job_urls


if __name__ == '__main__':
    print(process('https://www.manpower.com/'))
