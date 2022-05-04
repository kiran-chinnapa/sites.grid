import csv
import glob
import json
import logging
import threading
import time
import traceback
import urllib3
import csv_utils
import metadata
import grid_utils
import os
import argparse
import sites_column_mapper


class MyThread(threading.Thread):
    def __init__(self, _domain, _file_name):
        threading.Thread.__init__(self)
        self.domain = _domain
        self.insertDataDict = {"insert": {"rows": []}}
        self.rowDict = {}
        # self.insert_idx = 0
        self.meta_data_obj = metadata.Metadata()
        self.sites_col_mapper = sites_column_mapper.Mapper(self.meta_data_obj)
        self.file_name = _file_name

    def run(self):
        try:
            logging.info(str(threading.get_ident()) + ":thread processing domain:" + self.domain)
            # lst = self.file_name.split("/")
            # file_name = lst[len(lst) - 1]
            # logging.info("processing file: " + self.file)
            # with open(self.file) as read_obj:
            #     csv_reader = csv.reader(read_obj)
            #     for csv_row in csv_reader:
            resp_url = self.meta_data_obj.get_resp_url(self.domain)
            if self.meta_data_obj.get_language(resp_url.resp) in ['english','en','en-us']:
                self.sites_col_mapper.map(self.rowDict, self.file_name, self.domain, resp_url)
                self.insertDataDict["insert"]["rows"].append(self.rowDict.copy())

                # if self.insert_idx % max_rows_to_insert == 0:
                grid_utils.GridUtils.add_row(grid_id, auth_id, self.insertDataDict, 0, str(threading.get_ident()))
                # csv_utils.CsvUtils.add_row_to_csv(self.insertDataDict, self.insert_idx, "sites_metadata.csv")
                self.insertDataDict["insert"]["rows"] = []
                #logging.info('last domain inserted:' + csv_row[2])
                # self.insert_idx = self.insert_idx + 1
                self.rowDict = {}

            # if len(self.insertDataDict["insert"]["rows"]) > 0:
            #     grid_utils.GridUtils.add_row(grid_id, auth_id, self.insertDataDict, self.insert_idx, str(threading.get_ident()))
                # csv_utils.CsvUtils.add_row_to_csv(self.insertDataDict, self.insert_idx, "sites_metadata.csv")
            logging.info(str(threading.get_ident()) + ":thread completed :" + self.domain)
            with open(".completed", "a") as completed_file:
                completed_file.write(self.domain+",")
        except:
            traceback.print_exc()


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='sites_grid_loader.log')

max_rows_to_insert = 20
#test grid id
# grid_id = '6076de7b94a030356bf5ee2b'
auth_id = 'b1427e2f-81f4-4280-b56b-305ba8f36b5c'
grid_id = '60c2f76294a0307b4461a17b'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser()
parser.add_argument(dest="files_path",type=str, help="please pass csv file path, example \"/home/ubuntu/kiran/active-sample/*.csv\"")
parser.add_argument(dest="thread_size",type=str, help="please pass thread size example 200")
args = parser.parse_args()

delete_json_str = '{ "delete": { "query": { "globalFilter": { "filters": ' \
                       '[ { "operator": "LIKE", "keyword": "[a-zA-Z0-9_]" } ] } } } }'

if not os.path.exists(os.getcwd()+"/.completed"):
    grid_utils.GridUtils.truncate(grid_id, auth_id, delete_json_str)

start = time.time()
threads = []
i = 0
files_path = args.files_path
thread_size = args.thread_size

logging.info("sites.grid loader process started")

with open(".completed","r") as comp_file:
    comp_list = comp_file.read().split(",")

for file in glob.glob(files_path):
    lst = file.split("/")
    file_name = lst[len(lst) - 1]
    logging.info("processing file: " + file_name)
    with open(file) as read_obj:
        csv_reader = csv.reader(read_obj)
        for csv_row in csv_reader:
            if csv_row[0] in comp_list: continue
            i = i + 1
            if i == int(thread_size):
                for t in threads:
                    t.join()
                i = 0
                threads = []
                logging.info(thread_size+" threads: completed")
            t = MyThread(csv_row[0], file_name)
            t.start()
            threads.append(t)
for t in threads:
    t.join()

logging.info("total processing time elapsed seconds:" + str(time.time() - start))



