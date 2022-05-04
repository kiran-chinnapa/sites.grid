import csv
import json
import sys

from crawlers.utils import grid_utils


# grid_utils.search_grid
def get_all_error_types():
    error_list = []
    with open('../resources/error_types') as f:
        reader = csv.DictReader(f, delimiter=",")
        for row in reader:
            error_list.append(row['error_type'])
    return error_list


errors = get_all_error_types()
dmv_query = '''{
    "query": {
        "columnFilter": {
            "filters": [
                {
                    "column": "Spider Name",
                    "operator": "EQ",
                    "keyword": "url-extractor"
                }
            ]
        }
    },
    "distinct": {
        "columnNames": [
            "Error Reason"
        ]
    }
}'''

cp_query = '''{
    "query": {
        "columnFilter": {
            "filters": [
                {
                    "column": "Spider Name",
                    "operator": "EQ",
                    "keyword": "job-postings-extractor"
                }
            ]
        }
    },
    "distinct": {
        "columnNames": [
            "Error Reason"
        ]
    }
}'''


val = input("Which error counts do you want? enter 1 for dmv companies, 2 for career page : ")
print(val)
spider_name = ""

if "1" == val:
    spider_name = "url-extractor"
    distinct_errors = grid_utils.get_distinct_rows_for_query(dmv_query, grid_utils.qa_uncrawl_grid_id)
    print('dmv company errors count :{}'.format(len(distinct_errors)))
elif "2" == val:
    spider_name = "job-postings-extractor"
    distinct_errors = grid_utils.get_distinct_rows_for_query(cp_query, grid_utils.qa_uncrawl_grid_id)
    print('career page errors count :{}'.format(len(distinct_errors)))

new_error_set = set()
# check if there are new errors
for d_error in distinct_errors:
    new_error = True
    for e in errors:
        if e in d_error:
            new_error = False
            break
        else:
            continue
    if new_error:
        # print('new error found : {}'.format(d_error))
        new_error_set.add(d_error[:25])

for new_err in new_error_set:
    print('new error found : {}'.format(new_err))

if new_error_set:
    sys.exit(0)

error_search = '''{
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "Error Reason",
                        "operator": "LIKE",
                        "keyword": ""
                    },
                    {
                        "column": "Spider Name",
                        "operator": "LIKE",
                        "keyword": ""
                    }
                ]
            },
            "showColumnNamesInResponse": true
        }
    }'''

for error in errors:
    error_search_dict = json.loads(error_search)
    error_search_dict['query']['columnFilter']['filters'][0]['keyword'] = error
    error_search_dict['query']['columnFilter']['filters'][1]['keyword'] = spider_name
    search_result = grid_utils.global_search(
        grid_utils.qa_uncrawl_grid_id, grid_utils.qa_auth_id, json.dumps(error_search_dict))
    if search_result['rows']:
        print('{}${}$source grid={},source grid Id={},source company url={}$ {}'.
              format(error, search_result['totalRowCount'],
                     search_result['rows'][0]['dmv_grid_name'],
                     search_result['rows'][0]['dmv_grid_id'],
                     search_result['rows'][0]['company_url'],
                     search_result['rows'][0]['Error Reason']))
    # print('source grid:{}'.format(search_result['rows'][0]['dmv_grid_name']))
    # print('source grid Id:{}'.format(search_result['rows'][0]['dmv_grid_id']))
    # print('source company url:{}'.format(search_result['rows'][0]['company_url']))
