# Code flow
# 1. Loop through all the DMV grids
# 2. For each row extract the company name and website
# 3. Find careers page for the company using keywords /careers, /jobs or /careers/search or /jobs/search using next steps.
# 4. Goto company website with suffix keywords /careers, /jobs, /careers/search, /jobs/search to find the job posting page.
# 5. If not found by step 4 crawl the sitemaps to see for pages with keywords mentioned in step 4.
# 6. Use google json or job description page regex to find the job posting description page.
# 7. Once found iterate through all the pages and extract the job details for each job postings
# 8. If google json is available then scrape using Structured Job Posting Extractor otherwise try extracting through regex or manual process mentioned in next step.
# 9. Save the company name and job listing page link to Company.grid.
# 10. Send list for Nithin to perform manual extraction.
import traceback
import json
import logging
import time
import grid_utils
import csv
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests,bs4

auth_id = 'ed0444ad-9e34-43b5-b0f5-5d08351a3def'
query_template = '{ "query": { "selectColumnNames": [], ' \
'"pagination": {},'\
                       '"sendRowIdsInResponse": true, "showColumnNamesInResponse": true}}'
query_count ='''{
    "query": {
        "globalFilter": {
            "filters": [
                {
                    "operator": "LIKE",
                    "keyword": "[a-zA-Z0-9]"
                }
            ]
        }
    }
}'''
json_dict = json.loads(query_template)
# json_dict['query']['selectColumnNames'] = ['dude','bro']
# print(json_dict)

def find_job_posting_page(company_name, website):
    pass


def main():
    with open('../resources/prod_dmv_grids') as file:
        csv_reader = csv.reader(file)
        next(csv_reader, None)
        for row in csv_reader:
            print("processing grid:{}".format(row[0]))
            total_row_count = grid_utils.global_search(grid_id=row[0], auth_id=auth_id,
                                                       search_query=query_count, search_type='search_count', env='prod')
            print(total_row_count)
            json_dict['query']['selectColumnNames'] = [row[1], row[2]]
            json_dict['pagination'] = {"startRow": 1, "rowCount": total_row_count}
            # print(json.dumps(json_dict))
            result = grid_utils.global_search(grid_id=row[0], auth_id=auth_id,
                                              search_query=json.dumps(json_dict), env='prod')
            # print(result['totalRowCount'])
            for rslt_rows in result['rows']:
                print('company name:{}, company website:{}'.format(rslt_rows[row[1]], rslt_rows[row[2]]))
                find_job_posting_page(rslt_rows[row[1]], rslt_rows[row[2]])
            print('********************')


if __name__ == '__main__':
    main()