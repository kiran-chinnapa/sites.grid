import sys
import time

from crawlers.utils import grid_utils

dmv_metas = grid_utils.get_dmv_grid_metadata()
comp_urls = grid_utils.get_company_urls(grid_utils.qa_career_grid_id)
dmv_company_cnt = len(dmv_metas)
career_crawl_cnt = len(comp_urls)


# def load_skipped_companies(grid_id):
#
#     cnt = 0
#     insertDataDict = {"insert": {"rows": []}}
#     # with open('../resources/uncrawled', 'w') as f:
#     for dmv_meta in dmv_metas:
#         if dmv_meta['company_url'] not in comp_urls and not grid_utils.is_row_present('company_url',
#                                                                                       dmv_meta['company_url'],
#                                                                                       grid_utils.qa_uncrawl_grid_id):
#             # print(dmv_meta)
#             # f.write("{}\n".format(dmv_meta))
#             insertDataDict["insert"]["rows"] = []
#             dmv_meta['Error Reason'] = 'regex did not find career page'
#             dmv_meta['Spider Name'] = 'stats'
#             insertDataDict["insert"]["rows"].append(dmv_meta)
#             grid_utils.add_row(grid_id, grid_utils.qa_auth_id, insertDataDict)
#             print('adding company to uncrawled:{}'.format(dmv_meta['company_url']))
#             cnt = cnt + 1
#     print('total skipped count:{}'.format(cnt))


# if len(sys.argv) > 1 and 'load_skipped' == sys.argv[1]:
#     load_skipped_companies(grid_utils.qa_uncrawl_grid_id)
#     time.sleep(5)

# print('total count of companies in dmv grids : {}'.format(dmv_company_cnt))
# unique_dmv_comp = set()
# for dmv in dmv_metas:
#     unique_dmv_comp.add(dmv['company_url'])
query = '''{
    "query": {
        "columnFilter": {
            "filters": [
                {
                    "column": "Spider Name",
                    "operator": "LIKE",
                    "keyword": "sitemap_pages"
                },
                {
                    "column": "Spider Name",
                    "operator": "LIKE",
                    "keyword": "url-extractor"
                }
            ]
        }
    }
}'''
problem_count = grid_utils.global_search(grid_utils.qa_uncrawl_grid_id, grid_utils.qa_auth_id, query, 'search_count')[
    'totalRowCount']
print('total count of dmv company websites having career page: {}'.format(career_crawl_cnt))
print('total count of dmv company websites having problems : {}'.format(problem_count))
print('total count of companies having no career page {}'.
      format(dmv_company_cnt - (career_crawl_cnt + problem_count)))
print('total count of dmv company websites in big parser: {}(Sum of all the counts above)'.format(dmv_company_cnt))
