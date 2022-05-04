# thread should take input as start row and row count,
# it should form a query to search the sites.grid and update the rows based on search criteria
# not yet implemented
import json
import logging
import os
import sys
import threading
import traceback
import grid_utils
import urllib3
from panda_load_pages import PandasLoadPages

class MyThread(threading.Thread):
    def __init__(self, start_row, row_count):
        threading.Thread.__init__(self)
        self.start_row = start_row
        self.row_count = row_count
        self.query = "{\"query\":{\"columnFilter\":{\"filters\":[{\"column\":\"Active\",\"operator\":\"EQ\",\"keyword\":\"Yes\"}]}," \
                  "\"selectColumnNames\":[\"Domain\",\"Site Map Url\"],\"pagination\":{\"startRow\":"\
                     +self.start_row+",\"rowCount\":"+self.row_count+"},\"sendRowIdsInResponse\":true}}"
        self.rowDict = {}
        self.insert_idx = 0
        self.insertDataDict = {"insert": {"rows": [{}]}}

    def insert_to_pages(self, s_rows):
        for s_row in s_rows:
            logging.info(str(threading.get_ident())+":processing sitemap:"+s_row[2])
            try:
                dataframe = PandasLoadPages.parse_sitemap(s_row[2], ["loc", "lastmod"], None, None)
            except:
                logging.info('no pages for sitemap url:' + s_row[2])
                continue
            if dataframe.empty:
                logging.info('no pages for sitemap url:' + s_row[2])
                continue
            for index, row in dataframe.iterrows():
                self.rowDict["Domain"] = s_row[1]
                self.rowDict["Site Map Url"] = s_row[2]
                self.rowDict["Pages"] = row['loc']
                self.rowDict["Last Modified"] = row['lastmod']
                self.insertDataDict["insert"]["rows"].append(self.rowDict.copy())
                if self.insert_idx % max_rows_to_insert == 0:
                    json_object = json.dumps(self.insertDataDict, indent=4)
                    grid_utils.GridUtils.add_row(pages_grid_id, auth_id, json_object, self.insert_idx)
                    self.insertDataDict["insert"]["rows"] = []
                    logging.info('last domain inserted:' + s_row[1])
                self.insert_idx = self.insert_idx + 1
                self.rowDict = {}

        if len(self.insertDataDict["insert"]["rows"]) > 0:
            json_object = json.dumps(self.insertDataDict, indent=4)
            grid_utils.GridUtils.add_row(pages_grid_id, auth_id, json_object, self.insert_idx)



    def run(self):
        try:
            logging.info(str(threading.get_ident()) + ":thread processing start row:" + self.start_row)
            json_rslt = grid_utils.GridUtils.search(sites_grid_id, auth_id, self.query)
            #print("start:count->" + self.start_row + ":" + self.row_count)
            # print(json_rslt['rows'])
            self.insert_to_pages(json_rslt['rows'])
            logging.info(str(threading.get_ident()) + ":thread completed :" + self.start_row + ": inserted:" + str(self.insert_idx))
            pages_comp_file.write(self.start_row + ",")
        except:
            traceback.print_exc()


sites_grid_id = "6086513f94a0301210483775"
auth_id = "b1427e2f-81f4-4280-b56b-305ba8f36b5c"
max_rows_to_insert = 2000
pages_grid_id = "6084301494a030356bf98c12"

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='pages_grid_loader.log')

delete_json_str = '{ "delete": { "query": { "globalFilter": { "filters": ' \
                       '[ { "operator": "LIKE", "keyword": "[a-zA-Z0-9_]" } ] } } } }'
if not os.path.exists(os.getcwd()+"/.pages_completed"):
    grid_utils.GridUtils.truncate(pages_grid_id, auth_id, delete_json_str)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
thread_size = int(sys.argv[1])
threads = []
s_row = 1
r_count = 10
# get total row count


t_row_cnt_query = "{\"query\":{\"columnFilter\":{\"filters\":[{\"column\":\"Active\",\"operator\":\"EQ\",\"keyword\":\"Yes\"}]}}}"
total_row_cnt = grid_utils.GridUtils.search_count(sites_grid_id,auth_id,t_row_cnt_query)
i = 0
# for every n threads do join, do it upto total row count
with open(".sites_completed", "r+") as pages_comp_file:
    lst = pages_comp_file.read().split(",")
    for st_row in range(s_row, total_row_cnt, r_count):
        i = i + 1
        if str(st_row) in lst:
            continue
        if i % thread_size == 0:
            for t in threads:
                t.join()
            threads = []
        t = MyThread(str(st_row), str(r_count))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
