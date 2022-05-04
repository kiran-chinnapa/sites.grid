import os
import re
import sys
import time
from scrapy.http import HtmlResponse
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import logging
from crawlers.utils import grid_utils, crawl_utils
import html2text


class JobMetadataParser:

    def __init__(self, chrome_driver):
        self.chrome_driver = chrome_driver

    def get_scrapy_response(self, url):
        try:
            self.chrome_driver.get(url)
            time.sleep(5)
            body = self.chrome_driver.page_source
            return HtmlResponse(url=self.chrome_driver.current_url, body=body, encoding='utf-8')
        except Exception as e:
            raise Exception('chrome_driver unable to process url :{} exception :{}'.format(url, str(e)))

    def map_job_columns(self, job_title_url, meta_json):
        resp = self.get_scrapy_response(job_title_url)
        html_text = html2text.html2text(resp.text)
        html_text = self.clean_html(html_text)
        # self.write_to_file(html_text, job_title_url.split('/')[2])
        # return
        if meta_json.get("Job Title") is None:
            meta_json["Job Title"] = self.parse_regex_value(html_text, r"\A.*")
        meta_json["Job Page"] = job_title_url
        meta_json["Job Description"] = self.extract_metadata(html_text, r"job description|description|responsibilities")
        meta_json["Minimum qualification"] = self.extract_metadata(html_text,
                                                                   r"minimum qualifications|minimum|required skills")
        meta_json["Preferred qualification"] = self.extract_metadata(html_text,
                                                                     r"qualification|preferred qualification|technical expertise|desired skills")
        meta_json["Job Type"] = ' '.join(
            re.findall(r"full-time|part-time|full time|part time|individual contributor|contract", html_text,
                       re.IGNORECASE))
        meta_json["Benefits"] = self.extract_metadata(html_text, r"benefits")
        return

    def clean_html(self, html_text):
        html_text = html_text.replace('\n\n', '\n').replace('#', ' ').replace('*', ' ')
        clean = []
        for line in html_text.split('\n'):
            if '[' not in line or ']' not in line:
                clean.append(line)
        return '\n'.join(clean)

    def parse_regex_value(self, html_text, _regex):
        match = re.search(_regex, html_text)
        return match.group() if match is not None else ""

    def extract_metadata(self, text, rgx):
        jd_list = []
        lines = text.split('\n')
        found = False
        for line in lines:
            if re.search(rgx, line, re.IGNORECASE):
                found = True
            if found:
                _match = re.search(rgx, line, re.IGNORECASE)
                if len(line.split()) <= 2:
                    if _match:
                        continue
                    else:
                        break
                elif line not in jd_list:
                    jd_list.append(line)
        if found:
            return '\n'.join(jd_list)
        return ''

    def write_to_file(self, text, file_name):
        with open('../resources/jobs/{}'.format(file_name), 'w') as f:
            f.write(text)
            print('file {} write completed'.format(file_name))


if __name__ == '__main__':
    chrome_driver= crawl_utils.get_driver()
    metadata_parser = JobMetadataParser(chrome_driver)
    out_json = {}
    urls = [
        # "https://careers.microsoft.com/us/en/job/1257279/B-BBEE-Executive",
        # "https://jobs.apple.com/en-in/details/200339005/senior-software-engineer-data?team=SFTWR",
        # "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions/preview/210008620",
        # "https://hdpc.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/requisitions/preview/74003/?sortBy=POSTING_DATES_DESC",
        # "https://www.metacareers.com/v2/jobs/850252232095802/",
        # "https://careers.bankofamerica.com/en-us/job-detail/21074298/-net-mvc-webforms-developer-multiple-locations",
        # "https://www.amazon.jobs/en/jobs/996246/senior-software-dev-engineer",
        # "https://www.accenture.com/us-en/careers/jobdetails?id=R00075368_en&title=Federal+-+Full-Stack+Software+Developer",
        "http://jobs.jobvite.com/nmr-consulting/job/onPwifwK"
    ]
    for url in urls:
        metadata_parser.map_job_columns(url, out_json)
        print(out_json['Job Title'])
        # print(out_json["Job Title"])
        # print(out_json['Job Description'])
        # print(out_json['Minimum qualification'])
        # print(out_json["Preferred qualification"])
        # print(out_json['Job Type'])
        # print(out_json['Benefits'])
        # print('*************************************')
        # print(json.dumps(out_json, indent=4))
        # with open('../resources/out.json', 'w') as f:
        #     # f.write(json.dumps(out_json, indent=4))
        #     f.write(str(out_json))
        # insertDataDict = {"insert": {"rows": []}}
        # insertDataDict["insert"]["rows"].append(out_json)
        # grid_utils.add_row('6200f79f94a0302330aa7e9a', grid_utils.qa_auth_id, insertDataDict)
    chrome_driver.quit()
