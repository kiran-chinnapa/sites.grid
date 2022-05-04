from crawlers.utils import grid_utils

search_uncrawled = '''{
    "query": {
        "columnFilter": {
            "filters": [
                {
                    "column": "Spider Name",
                    "operator": "EQ",
                    "keyword": "job-postings-extractor"
                }
            ]
        },
        "selectColumnNames": [
            "career_page_url"
        ],
        "pagination": {
            "startRow": 1,
            "rowCount": 3000
        }
    }
}'''

search_job_posting = '''{
    "query": {
        "columnFilter": {
            "filters": [
                {
                    "column": "Career Page",
                    "operator": "LIKE",
                    "keyword": "[A-Za-z]"
                }
            ]
        },
        "selectColumnNames": [
            "Career Page"
        ],
        "pagination": {
            "startRow": 1,
            "rowCount": 3500
        }
    }
}'''


cps_uncrawled = grid_utils.global_search(grid_utils.qa_uncrawl_grid_id,grid_utils.qa_auth_id,search_uncrawled)
job_posting_cps = grid_utils.global_search(grid_utils.qa_job_posting_page_grid,grid_utils.qa_auth_id,search_job_posting)

jps = []
for row in job_posting_cps['rows']:
    jps.append(row[0])

print('count of jps : {} '.format(len(jps)))

count = 0
for uncrawl in cps_uncrawled['rows']:
    if uncrawl[0] in jps:
        print(uncrawl)
        count = count + 1

print('count of uncrawled in jps: {}'.format(count))


