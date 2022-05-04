import csv
import json
import os
import time
import traceback
import requests
import logging

company_urls = '''{
  "query": {},
  "distinct": {"columnNames" :["Company Website"]}
}'''

query_distinct = '''
{
    "query": {},
    "distinct": {
        "columnNames": []
    }
}
'''

query_count = '''{
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
search_grid = '''{
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "",
                        "operator": "LIKE",
                        "keyword": ""
                    }
                ]
            },
            "showColumnNamesInResponse": true
        }
    }'''

search_career_grid = '''{
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "",
                        "operator": "LIKE",
                        "keyword": ""
                    }
                ]
            },
            "selectColumnNames": ["Company Name"],
            "showColumnNamesInResponse": true
        }
    }'''

search_column_filter = '''{
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "",
                        "operator": "EQ",
                        "keyword": ""
                    }
                ]
            }
        }
    }'''

get_career_links = '''{
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "Company Website",
                        "operator": "EQ",
                        "keyword": ""
                    }
                ]
            },
            "showColumnNamesInResponse": true
        }
    }'''

update_query = '''{
  "update": {
    "rows": []
  }
}'''

query_template = '{ "query": { "selectColumnNames": [], ' \
                 '"pagination": {},' \
                 '"sendRowIdsInResponse": true, "showColumnNamesInResponse": true}}'
global_filter_query = '''{
  "query": {
    "globalFilter": {
      "filters": [
       {
         "operator": "LIKE",
         "keyword": "[a-zA-Z0-9]"
       }
      ]
    },
    "pagination": {},
    "sendRowIdsInResponse": true,
    "showColumnNamesInResponse": true
  }
}'''

qa_career_grid_id = '61b573a994a0301ce5a001ce'
qa_jp_grid_id = '61b9f6be94a0301ce5a00d86'
qa_job_desc_grid_id = '61cc9f4694a0300e0e4c2933'
qa_auth_id = '7ebe66f0-e8cc-4238-b5b2-b627e86df906'
qa_uncrawl_grid_id = '61cec75a94a0300e0e4c6b83'
qa_top_companies_grid = '61e963f094a030072ce38425'
qa_job_posting_page_grid = '61f0488394a030480c082e51'
qa_jobs_grid_id = '6200f79f94a0302330aa7e9a'
qa_top_companies_jpp = '620f9e5c94a0300fedf6bdaa'
prod_jobs_grid_id = '6224e8a0c9d0822ec65fcb62'
prod_auth_id = '310dcccb-52ea-4184-b3b2-1258495d61a5'
latest_jobs_grid_id = '622734a594a0306ca6afb084'


def query_metadata(grid_id, env='qa'):
    domain = 'www.bigparser.com' if env == 'prod' else 'qa.bigparser.com'
    json_str = ''
    try:
        resp = requests.get(
            'https://{}/api/v2/grid/{}/query_metadata'.format(domain, grid_id))
        json_str = resp.json()
    except:
        traceback.print_exc()
    return json_str


def get_distinct_rows(column_name, grid_id):
    distinct_dict = json.loads(query_distinct)
    distinct_dict['distinct']['columnNames'].append(column_name)
    search_result = global_search(
        grid_id, qa_auth_id, json.dumps(distinct_dict), 'distinct')
    return search_result['matchingValues']


def get_distinct_rows_for_query(query, grid_id):
    search_result = global_search(
        grid_id, qa_auth_id, query, 'distinct')
    return search_result['matchingValues']


def global_search(grid_id, auth_id, search_query, search_type='search', env='qa'):
    domain = 'www.bigparser.com' if env == 'prod' else 'qa.bigparser.com'
    retry_cnt = 0
    if search_type == 'search_count':
        json_dict = {'totalRowCount': 0}
    elif search_type == 'search':
        json_dict = {'rows': []}
    elif search_type == 'distinct':
        json_dict = {'matchingValues': []}
    else:
        json_dict = {}
    try:
        # json_str = json.dumps(json.loads(search_query))
        headers = {
            'authId': auth_id,
            'Content-Type': 'application/json',
        }
        resp = requests.post('https://{}/api/v2/grid/{}/{}'.format(domain, grid_id, search_type),
                             headers=headers, data=search_query)
        if resp.status_code == 200:
            # logging.info("Search results found")
            json_dict = json.loads(resp.text)
            resp.close()
            return json_dict
        else:
            while retry_cnt < 5:
                resp = requests.post('https://{}/api/v2/grid/{}/{}'.format(domain, grid_id, search_type),
                                     headers=headers, data=search_query)
                if resp.status_code == 200:
                    # logging.info("Search results found")
                    json_dict = json.loads(resp.text)
                    resp.close()
                    return json_dict
                retry_cnt = retry_cnt + 1
            logging.error('no response after retry')
            logging.error(resp.text)
            logging.error(search_query)
            resp.close()
            return json_dict
    except:
        traceback.print_exc()
        return json_dict


def add_row(grid_id, auth_id, insertDataDict, index=0, thread_id=0, env='qa'):
    domain = 'www.bigparser.com' if env == 'prod' else 'qa.bigparser.com'
    retry_cnt = 5
    try:
        if "1" == os.getenv("disable_grid"):
            return

        json_object = json.dumps(insertDataDict)
        qaHeaders = {
            'authId': auth_id,
            'Content-Type': 'application/json',
        }
        # create_response = requests.post('https://qa.bigparser.com/api/v2/grid/' + grid_id + '/rows/bulk_create',
                                        # headers=qaHeaders, data=json_object)
        create_response = requests.post('https://{}/api/v2/grid/{}/rows/bulk_create'.format(domain, grid_id),
                                        headers=qaHeaders, data=json_object)

        if "1" == os.getenv("debug_flag"):
            print("json len {} thread id {} ".format(json_object, thread_id))
            exit(0)

        if create_response.status_code == 200 and create_response.json()['noOfRowsFailed'] == 0:
            # logging.info("json object:{}".format(json_object))
            logging.info("Success Rows Inserted:" + json_object)
            create_response.close()
            return True
        else:
            if 5 == retry_cnt:
                # logging.info("json obj: {}".format(json_object))
                # logging.info("Failed Rows Insertion:retrying after sleep: "+ str(index))
                logging.error("Failed Rows Insertion:process stopped: resp code:{} insertDataDict_len {}".format(
                    create_response.json(), index))
                logging.error(json_object)
            logging.error("Failed Rows Insertion: retrying:resp code:{}".format(create_response.json()))
            time.sleep(1)
            retry_cnt += 1
            add_row(grid_id, auth_id, json_object, index)
    except:
        create_response.close()
        traceback.print_exc()


delete_grid = '{ "delete": { "query": { "globalFilter": { "filters": ' \
              '[ { "operator": "LIKE", "keyword": "[a-zA-Z0-9_]" } ] } } } }'

delete_by_rowId = '''{
    "delete": {
        "rows": [
            {
                "rowId": ""
            }
        ]
    }
}'''


def clear_duplicates(grid_id, column_name):
    unique_career_urls = get_distinct_rows(column_name, grid_id)
    print('count of unique {} :{}'.format(column_name, len(unique_career_urls)))
    career_links_dict = json.loads(query_career_row_ids)
    career_links_dict['query']['columnFilter']['filters'][0]['column'] = column_name
    delete_dict = json.loads(delete_by_rowId)
    for unique_career_url in unique_career_urls:
        # if unique_career_url.startswith('http'): unique_career_url = unique_career_url.split('//')[1]
        # if unique_career_url.endswith('/'): unique_career_url = unique_career_url[:-1]

        career_links_dict['query']['columnFilter']['filters'][0]['keyword'] = unique_career_url
        json_result = global_search(grid_id=grid_id, auth_id=qa_auth_id,
                                    search_query=json.dumps(career_links_dict))

        if json_result['totalRowCount'] > 1:
            print('total row count for {} is {}'.format(unique_career_url, json_result['totalRowCount']))
            list_iterator = iter(json_result['rows'])
            next(list_iterator)
            for row in list_iterator:
                delete_dict['delete']['rows'][0]['rowId'] = row['_id']
                print('deleting: {}'.format(delete_dict))
                delete_by_row_id(grid_id, qa_auth_id, json.dumps(delete_dict))


def clear_career_page_junk():
    delete_by_column = '''{
    "delete": {
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "Career Page",
                        "operator": "EQ",
                        "keyword": ""
                    }
                ]
            }
        }
    }
}'''
    unique_career_urls = get_distinct_rows("Career Page", qa_career_grid_id)
    print('count of unique {} :{}'.format("Career Page", len(unique_career_urls)))
    delete_dict = json.loads(delete_by_column)
    upper_list = []
    for unique_career_url in unique_career_urls:
        for u in unique_career_url:
            if u.isupper():
                upper_list.append(unique_career_url.lower())

    print('upper list len :{}'.format(len(upper_list)))

    for unique_career_url in unique_career_urls:
        if unique_career_url in upper_list:
            delete_dict['delete']['query']["columnFilter"]["filters"][0]["keyword"] = unique_career_url
            print('deleting: {}'.format(delete_dict))
            delete_all_rows(qa_career_grid_id, qa_auth_id, json.dumps(delete_dict))


def delete_all_rows(grid_id, auth_id, query):
    # manually delete rows from grid

    if "1" == os.getenv("disable_grid"):
        return
    qaHeaders = {
        'authId': auth_id,
        'Content-Type': 'application/json',
    }
    response = requests.delete('https://qa.bigparser.com/api/v2/grid/' + grid_id + '/rows/delete_by_queryObj',
                               headers=qaHeaders, data=query)
    if 200 == response.status_code:
        logging.info("all rows deleted")
        response.close()
        return True
    else:
        logging.info("no rows deleted:" + response.text)
        response.close()
        return False


def delete_by_row_id(grid_id, auth_id, query):
    # manually delete rows from grid

    if "1" == os.getenv("disable_grid"):
        return
    qaHeaders = {
        'authId': auth_id,
        'Content-Type': 'application/json',
    }
    response = requests.delete('https://qa.bigparser.com/api/v2/grid/' + grid_id + '/rows/delete_by_rowIds',
                               headers=qaHeaders, data=query)
    if 200 == response.status_code:
        logging.info("all rows deleted")
        response.close()
        return True
    else:
        logging.info("no rows deleted:" + response.text)
        response.close()
        return False


def get_company_urls(grid_id):
    distinct_urls = global_search(grid_id=grid_id, auth_id=qa_auth_id, search_query=company_urls,
                                  search_type='distinct')
    return distinct_urls['matchingValues']


def get_all_rows(company_url=None, grid_id=None):
    career_links_dict = json.loads(get_career_links)
    career_links_dict['query']['columnFilter']['filters'][0]['keyword'] = company_url
    json_result = global_search(grid_id=grid_id, auth_id=qa_auth_id,
                                search_query=json.dumps(career_links_dict))
    return json_result['rows']


search_career_row_ids = '''{
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "Company Website",
                        "operator": "LIKE",
                        "keyword": ""
                    }
                ]
            },
            "selectColumnNames": ["_id"],
            "sendRowIdsInResponse": true,
            "showColumnNamesInResponse": true
        }
    }'''

query_career_row_ids = '''{
        "query": {
            "columnFilter": {
                "filters": [
                    {
                        "column": "",
                        "operator": "EQ",
                        "keyword": ""
                    }
                ]
            },
            "selectColumnNames": ["_id"],
            "sendRowIdsInResponse": true,
            "showColumnNamesInResponse": true
        }
    }'''


def get_career_row_ids(company_url):
    career_links_dict = json.loads(search_career_row_ids)
    career_links_dict['query']['columnFilter']['filters'][0]['keyword'] = company_url
    json_result = global_search(grid_id=qa_career_grid_id, auth_id=qa_auth_id,
                                search_query=json.dumps(career_links_dict))
    return json_result['rows']


def update_grid(grid_id, auth_id, update_dict, env='qa'):
    try:
        domain = 'www.bigparser.com' if env == 'prod' else 'qa.bigparser.com'
        query = json.dumps(update_dict)
        qaHeaders = {
            'authId': auth_id,
            'Content-Type': 'application/json',
        }
        create_response = requests.put('https://{}/api/v2/grid/{}/rows/update_by_rowIds'.format(domain, grid_id),
                                       headers=qaHeaders, data=query)
        retry_cnt = 0

        if create_response.status_code == 200:
            # logging.info("json object:{}".format(json_object))
            logging.info("Success Rows Updated")
            create_response.close()
            return True
        else:
            if 5 == retry_cnt:
                logging.info("Failed Rows Update:process stopped: resp code:{} insertDataDict_len {}".format(
                    create_response.status_code))
                logging.info(query)
                exit(0)
            logging.info("Failed Rows Insertion: retrying:resp code:{}".format(create_response.status_code))
            time.sleep(1)
            retry_cnt += 1
            update_grid(grid_id, auth_id, update_dict)
    except:
        create_response.close()
        traceback.print_exc()


def get_all_dmv_grids():
    dmv_grids_list = []
    with open('../resources/prod_dmv_grids') as f:
        reader = csv.DictReader(f, delimiter=",")
        for row in reader:
            dmv_grids_list.append(row)
    return dmv_grids_list


def search_in_dmv_grids(company):
    career_search = json.loads(search_career_grid)
    for dmv_grid in get_all_dmv_grids():
        career_search['query']['columnFilter']['filters'][0]['column'] = dmv_grid['website_column']
        career_search['query']['columnFilter']['filters'][0]['keyword'] = company.split('/')[2]
        result_dict = global_search(dmv_grid['grid_id'], prod_auth_id,
                                    json.dumps(career_search), env='prod')

        if result_dict['totalRowCount'] > 0:
            print(dmv_grid['grid_id'])
            print(result_dict)


def get_dmv_grid_metadata():
    dmv_metas = []
    json_dict = json.loads(query_template)
    dmv_grids = get_all_dmv_grids()
    for dmv_grid in dmv_grids:
        total_row_count = global_search(grid_id=dmv_grid['grid_id'], auth_id=prod_auth_id,
                                        search_query=query_count,
                                        search_type='search_count',
                                        env='prod')
        json_dict['query']['selectColumnNames'] = [dmv_grid['company_column'], dmv_grid['website_column'],
                                                   'Company Name']
        json_dict['query']['pagination'] = {"startRow": 1, "rowCount": total_row_count['totalRowCount']}
        result = global_search(grid_id=dmv_grid['grid_id'], auth_id=prod_auth_id,
                               search_query=json.dumps(json_dict), env='prod')
        for rslt_rows in result['rows']:
            company_website = rslt_rows[dmv_grid['website_column']].strip()
            if not company_website.startswith('http'):
                company_website = 'http://{}'.format(company_website)
            dmv_meta = {}
            dmv_meta['dmv_grid_name'] = dmv_grid['grid_name']
            dmv_meta['dmv_grid_id'] = dmv_grid['grid_id']
            dmv_meta['company_url'] = company_website
            dmv_meta['company_name'] = rslt_rows['Company Name']
            dmv_metas.append(dmv_meta)
    return dmv_metas


def is_row_present(col_val_dict, grid_id, auth_id, envt):
    _query = '''{
            "query": {
                "columnFilter": {
                    "filters": []
                },
                "showColumnNamesInResponse": true
            }
        }'''
    query_dict = json.loads(_query)
    for col, val in col_val_dict.items():
        query_dict['query']['columnFilter']['filters'].append(
            {"column": col, "operator": "EQ", "keyword": val}
        )
    total_row_cnt = global_search(grid_id=grid_id, auth_id=auth_id,
                                  search_query=json.dumps(query_dict),
                                  search_type='search_count',env=envt)
    if total_row_cnt['totalRowCount'] > 0:
        logging.info('row already present: {}'.format(col_val_dict))
        return True
    return False


def get_uncrawled_companies():
    dmv_metas = get_dmv_grid_metadata()
    # dmv_meta_set = set()
    # for dmv_meta in dmv_metas:
    #     dmv_meta_set.add(dmv_meta['company_url'])
    comp_urls = set(get_company_urls(qa_career_grid_id))
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
    },
    "distinct": {
        "columnNames": [
            "company_url"
        ]
    }
    }'''
    problem_urls = global_search(qa_uncrawl_grid_id, qa_auth_id, query, 'distinct')['matchingValues']
    uncrawled_comps = []
    for dmv_meta in dmv_metas:
        if dmv_meta['company_url'] not in comp_urls and dmv_meta['company_url'] not in problem_urls:
            uncrawled_comps.append(dmv_meta)
    return uncrawled_comps


def get_grid_rows(grid_id):
    count_query = '''{
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

    query = '''{
        "query": {
            "pagination": {
                "startRow": 1,
                "rowCount": ""
            },
            "showColumnNamesInResponse": true
        }
    }'''
    count = global_search(grid_id, qa_auth_id, count_query, 'search_count')['totalRowCount']
    json_dict = json.loads(query)
    json_dict['query']['pagination']['rowCount'] = count
    return global_search(grid_id, qa_auth_id, json.dumps(json_dict), 'search')['rows']


def delete_career_job_posting_same():
    delete_by_column = '''{
        "delete": {
            "query": {
                "columnFilter": {
                    "filters": [
                        {
                            "column": "Career Page",
                            "operator": "EQ",
                            "keyword": ""
                        }
                    ]
                }
            }
        }
    }'''
    jp_grid_rows = get_grid_rows(qa_job_posting_page_grid)
    count = 0
    delete_dict = json.loads(delete_by_column)
    for jp_grid_row in jp_grid_rows:
        if jp_grid_row['Career Page'] == jp_grid_row['Job Posting Page']:
            print(jp_grid_row['Career Page'])
            count = count + 1
            # delete_dict['delete']['query']["columnFilter"]["filters"][0]["keyword"] = jp_grid_row['Job Posting Page']
            # print('deleting: {}'.format(delete_dict))
            # delete_all_rows(qa_job_posting_page_grid, qa_auth_id, json.dumps(delete_dict))
    print("total count:{}".format(count))


def get_good_company_rows():
    cp_urls = set()
    with open('../resources/Career_Page_grid.csv') as f:
        reader = csv.DictReader(f, delimiter=",")
        for row in reader:
            cp_urls.add(row['Company Website'])
    return cp_urls


if __name__ == '__main__':
    # truncate(qa_job_desc_grid_id, qa_auth_id)
    # print(len(get_company_urls()))
    # print(get_all_dmv_grids())
    # print(get_career_row_ids('https://zoomph.com')[0]['_id'])
    # search_in_dmv_grids('https://www.sap.com')
    # for row in get_all_rows('https://www.laborfinders.com', qa_career_grid_id):
    #     print(row['Company Name'])
    delete_all_rows("622734a594a0306ca6afb084", qa_auth_id,delete_grid)
    # load_uncrawled_companies('61cec75a94a0300e0e4c6b83')
    # comp_urls = get_company_urls('61b573a994a0301ce5a001ce')
    # print('count::{}'.format(len(comp_urls)))
    # dmv_metas = get_dmv_grid_metadata()
    # print('dmv meta count::{}'.format(len(dmv_metas)))
    # uncrawled = get_uncrawled_companies()
    # print(uncrawled)
    # uncrawled_comps = set()
    # for dmv in uncrawled:
    #     uncrawled_comps.add(dmv['company_url'])
    #     if 'chesapeakeaerialphoto.com' in dmv['company_url']:
    #         print(dmv)
    #         break
    # print('length of uncrawled {}'.format(len(uncrawled)))
    # print('length of unique companies in uncrawled {}'.format(len(uncrawled_comps)))
    # clear_duplicates(qa_career_grid_id, 'Career Page')
    # clear_career_page_junk()
    # delete_career_job_posting_same()
    # print(len(get_good_company_rows()))
    # print(len(get_distinct_rows('Company Website',qa_career_grid_id)))
    # print(is_row_present({'company_url':'https://www.tts.services/','Spider Name':'url-extractor'},
    #                qa_uncrawl_grid_id))
    pass
