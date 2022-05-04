from crawlers.utils import grid_utils

comp_urls = grid_utils.get_company_urls(grid_utils.qa_career_grid_id)
comp_urls = set(comp_urls)
comp_urls_cnt = len(comp_urls)
print('count of companies having career pages: {}'.format(comp_urls_cnt))

job_page_comps = grid_utils.get_company_urls(grid_utils.qa_jp_grid_id)
job_page_comps = set(job_page_comps)
job_page_comps_cnt = len(job_page_comps)
print('count of companies having Job Page: {}'.format(job_page_comps_cnt))

dist_query ='''
{
    "query": {
        "columnFilter": {
            "filters": [
                {
                    "column": "Spider Name",
                    "operator": "EQ",
                    "keyword": "jp-link-extractor"
                }
            ]
        }
    },
    "distinct": {
        "columnNames": [
            "company_url"
        ]
    }
}
'''
job_page_errs = grid_utils.get_distinct_rows_for_query(dist_query, grid_utils.qa_uncrawl_grid_id)
job_page_errs = set(job_page_errs)

job_error_set = set()
for job_page_err in job_page_errs:
    if job_page_err in job_page_comps:
        # print('removing error company {} since it has some links with valid job'.format(job_page_err))
        pass
    else:
        job_error_set.add(job_page_err)

job_page_errs_cnt = len(job_error_set)
print('count of companies having errors getting job page: {}'.format(job_page_errs_cnt))

comp_not_crawled = []
for comp_url in comp_urls:
    if comp_url not in job_page_comps and comp_url not in job_page_errs:
        comp_not_crawled.append(comp_url)
        # print(comp_url)
print('total count of companies having no career page or career page embedded in java script : {}'.format(len(comp_not_crawled)))

job_page_total_comps = set()
job_page_total_comps.update(job_page_comps)
job_page_total_comps.update(job_page_errs)
job_page_total_comps.update(comp_not_crawled)
print('total companies crawled by Job Page Spider: {}'.format(len(job_page_total_comps)))

# for job_page_comp in job_page_total_comps:
#     if job_page_comp not in comp_urls:
#         print(job_page_comp)