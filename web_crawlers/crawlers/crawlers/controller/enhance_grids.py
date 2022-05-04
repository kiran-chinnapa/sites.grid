import json

from crawlers.utils import grid_utils


def enhance_career_page():
    """loop through distinct company urls from career page grid
    For each company url search all the dmv grids to get the name of the dmv grid.
    Update all rows of the company url in career page grid with the DMV grid name
    """
    career_search = json.loads(grid_utils.search_career_grid)
    update_dict = json.loads(grid_utils.update_query)
    company_urls = grid_utils.get_company_urls()
    dmv_grids = grid_utils.get_all_dmv_grids()
    for company_url in company_urls:
        for dmv_grid in dmv_grids:
            career_search['query']['columnFilter']['filters'][0]['column'] = dmv_grid['website_column']
            career_search['query']['columnFilter']['filters'][0]['keyword'] = company_url.split('/')[2]
            result_dict = grid_utils.global_search(dmv_grid['grid_id'], grid_utils.prod_auth_id,
                                                   json.dumps(career_search), env='prod')

            if result_dict['totalRowCount'] > 0:
                print(result_dict)
                rows = grid_utils.get_career_row_ids(company_url)
                for row in rows:
                    row_dict = {'rowId': row['_id'],
                                "columns": {'Company Name': result_dict['rows'][0]['Company Name'],
                                            'DMV Grid Id': dmv_grid['grid_id'],
                                            'DMV Grid Name': dmv_grid['grid_name']}}
                    update_dict['update']['rows'].append(row_dict)
                print(update_dict)
                grid_utils.update_grid(grid_utils.qa_career_grid_id, grid_utils.qa_auth_id, update_dict)
                update_dict['update']['rows'] = []
                break


def enhance_job_page():
    """loop through distinct career page urls from job page grid
    For each career page url search career page grid to get dmv grid name and company url
    Update all rows of the career page url in job page grid grid with DMV grid name and company url
    """


if __name__ == '__main__':
    enhance_career_page()
    # enhance_job_page()
