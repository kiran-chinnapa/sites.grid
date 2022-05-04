import json
import os
import sys
import time
import traceback
import requests
import logging
import counters
import metadata

retry_cnt =0

class GridUtils:
    def truncate(grid_id, auth_id, query):
        if "1" == os.getenv("disable_grid"):
            return
        qaHeaders = {
            'authId': auth_id,
            'Content-Type': 'application/json',
        }
        response = requests.delete('https://qa.bigparser.com/api/v2/grid/' + grid_id + '/rows/delete_by_queryObj',
                                   headers=qaHeaders, data=query)
        metadata.Metadata().req_resp_size_logger(response)
        if 200 == response.status_code:
            logging.info("all rows deleted")
            return True
        else:
            logging.info("no rows deleted:" + response.text)
            return False


    def add_row(grid_id, auth_id, insertDataDict, index, thread_id=0):

        if "1" == os.getenv("disable_grid"):
            return

        json_object = json.dumps(insertDataDict)
        qaHeaders = {
            'authId': auth_id,
            'Content-Type': 'application/json',
        }
        create_response = requests.post('https://qa.bigparser.com/api/v2/grid/' + grid_id + '/rows/bulk_create',
                                        headers=qaHeaders, data=json_object)
        metadata.Metadata().req_resp_size_logger(create_response)
        if "1" == os.getenv("debug_flag"):
            print("json len {} thread id {} ".format(json_object, thread_id))
            exit(0)

        if create_response.status_code == 200:
            # logging.info("json object:{}".format(json_object))
            logging.info("Success Rows Inserted:"+ str(index))
            return True
        else:
            global retry_cnt
            if 5 == retry_cnt:
                # logging.info("json obj: {}".format(json_object))
                # logging.info("Failed Rows Insertion:retrying after sleep: "+ str(index))
                logging.info("Failed Rows Insertion:process stopped: resp code:{} insertDataDict_len {}".format(create_response.status_code, index))
                logging.info(json_object)
                exit(0)
            logging.info("Failed Rows Insertion: retrying:resp code:{}".format(create_response.status_code))
            time.sleep(1)
            retry_cnt += 1
            GridUtils.add_row(grid_id, auth_id, json_object, index)


    def update_row(grid_id, auth_id, json_object, index):
        if "1" == os.getenv("disable_grid"):
            return
        qaHeaders = {
            'authId': auth_id,
            'Content-Type': 'application/json',
        }
        create_response = requests.post('https://qa.bigparser.com/api/v2/grid/' + grid_id + '/rows/bulk_create',
                                        headers=qaHeaders, data=json_object)
        metadata.Metadata().req_resp_size_logger(create_response)
        if create_response.status_code == 200:
            logging.info("Success Rows Inserted:" + str(index))
            return True
        else:
            # print(create_response.text)
            logging.info("Failed Rows Insertion:retrying after sleep: " + str(index))
            logging.info(json_object)
            time.sleep(5)
            GridUtils.add_row(grid_id, auth_id, json_object, index)


    def search(grid_id, auth_id, search_query, search_type):
        if "1" == os.getenv("disable_grid"):
            return

        try:
            json_str = json.dumps(json.loads(search_query))
            qaHeaders = {
                'authId': auth_id,
                'Content-Type': 'application/json',
            }
            resp = requests.post('https://qa.bigparser.com/api/v2/grid/{}/{}'.format(grid_id, search_type),
                                            headers=qaHeaders, data=json_str)
            metadata.Metadata().req_resp_size_logger(resp)
            if resp.status_code == 200:
                logging.info("Search results found")
                return json.loads(resp.text)
            else:
                print(resp.text)
                logging.error("Failed search:retrying after sleep: ")
                logging.info(search_query)
                time.sleep(5)
                GridUtils.search(grid_id, auth_id, search_query, "search")
        except:
            traceback.print_exc()

    def global_search(grid_id, auth_id, search_query, search_type='search', env='qa'):
        if "1" == os.getenv("disable_grid"):
            return
        domain = 'www.bigparser.com' if env == 'prod' else 'qa.bigparser.com'
        try:
            # json_str = json.dumps(json.loads(search_query))
            headers = {
                'authId': auth_id,
                'Content-Type': 'application/json',
            }
            resp = requests.post('https://{}/api/v2/grid/{}/{}'.format(domain, grid_id, search_type),
                                            headers=headers, data=search_query)
            metadata.Metadata().req_resp_size_logger(resp)
            if resp.status_code == 200:
                logging.info("Search results found")
                return json.loads(resp.text)
            else:
                print(resp.text)
                logging.error("Failed search:retrying after sleep: ")
                logging.info(search_query)
                time.sleep(5)
                GridUtils.search(grid_id, auth_id, search_query, "search")
        except:
            traceback.print_exc()

    def releaseDataFrame(dataframe, sitemap_url, grid_id, auth_id, max_rows_to_insert, resp_url_obj):
        if "1" == os.getenv("disable_grid"):
            return

        insertDataDict = {"insert": {"rows": []}}
        rowDict = {}
        dom = ""
        logging.info("row count of data-frame:{}".format(dataframe.shape[0]))

        for index, row in dataframe.iterrows():
            # logging.info("iterating dataframe")
            if row['loc'] is None or row['loc'].strip() == '':
                continue
            if row['loc'].startswith('http'):
                pg = row['loc']
            else:
                pg = '{}{}{}'.format(resp_url_obj.pcl,resp_url_obj.domain,row['loc'])
            rowDict["Domain"] = resp_url_obj.domain
            rowDict["Site Map Url"] = sitemap_url
            rowDict["Pages"] = pg
            rowDict["Last Modified"] = row['lastmod']
            insertDataDict["insert"]["rows"].append(rowDict.copy())
            counters.set_insert_idx_size(1)
            if counters.insert_idx % max_rows_to_insert == 0:
                GridUtils.add_row(grid_id, auth_id, insertDataDict, len(insertDataDict["insert"]["rows"]))
                insertDataDict["insert"]["rows"] = []
            rowDict = {}
        # dom = ""
        if len(insertDataDict["insert"]["rows"]) > 0:
            GridUtils.add_row(grid_id, auth_id, insertDataDict, len(insertDataDict["insert"]["rows"]))
            insertDataDict["insert"]["rows"] = []
        del dataframe


if __name__ == '__main__':
    # rDict = {}
    # print(rDict.get("dude"))
    # if os.getenv("debug_flag") == "1":
    #     print("dude")
    #     exit(0)
    # exit(0)
    #
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level=logging.INFO,
    #     datefmt='%Y-%m-%d %H:%M:%S',
    #     filename='grid_utils.log')
    # delete_query = '{ "delete": { "query": { "globalFilter": { "filters": ' \
    #                '[ { "operator": "LIKE", "keyword": "[a-zA-Z0-9_]" } ] } } } }'
    # GridUtils.truncate(sys.argv[1],'51964783-ec9f-4aab-847c-b450fdbfc48b', delete_query)
    # logging.info(
    #     "total bandwidth used in bytes sent:{} received:{}".format(counters.total_req_size, counters.total_resp_size))
    # 
    # insert_query = "{\"insert\":{\"rows\":[{\"Domain\":\"www.testdomain1.com\",\"Site Map Url\":\"www.testsitemap1.com\",\"Pages\":" \
    #                "\"www.testpage1.com\"},{\"Domain\":\"www.testdomain2.com\",\"Site Map Url\":\"www.testsitemap2.com\",\"Pages\":\"" \
    #                "www.testpage2.com\"}]}}"
    # print(GridUtils.add_row('6084301494a030356bf98c12','b1427e2f-81f4-4280-b56b-305ba8f36b5c',insert_query,1))

    # search_query = "{\"query\":{\"globalFilter\":{\"filters\":[{\"operator\":\"LIKE\",\"keyword\":\"[A-Za-z0-9]\"}]}}}"
    
    # GridUtils.search_count('6084301494a030356bf98c12','b1427e2f-81f4-4280-b56b-305ba8f36b5c',search_query, "search_count")

    search_query = '{ "query": { "selectColumnNames": [ "Company Website", "Company Name"], ' \
                   '"sendRowIdsInResponse": true, "showColumnNamesInResponse": true}}'
    grid_id = '5e8fe2cdc9d0821a1c9290e0/share/5e9b979ec9d0821a1c93f46e'
    auth_id = 'ed0444ad-9e34-43b5-b0f5-5d08351a3def'
    json = GridUtils.global_search(grid_id,auth_id,search_query,env='prod')
    print(json)

