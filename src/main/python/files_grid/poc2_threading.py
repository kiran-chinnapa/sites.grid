# The code below reads the 10 gb file and populates sites.grid.
# please pass the input file to parse, start index, truncate grid [1/0],
# validate domain sitemap [1/0]. Example python3 poc2.py com.zone 34 1 0

import json
import logging
import sys
import socket
import time
import requests
import threading
import traceback
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='poc2_threading.log')


class Thread(threading.Thread):

    def insert_rows(self, insert_data_dict, insert_idx):
        try:
            json_object = json.dumps(insert_data_dict, indent=4)
            if bool(insert_data_dict['insert']['rows']):
                # print("***************** insert ***************")
                # print(json_object)
                logging.info(self.thread_name+':Request Sent for Inserting rows :'+str(insert_idx))
                create_response = requests.post('https://qa.bigparser.com/api/v2/grid/' + qaSitesGridId +
                                                '/rows/bulk_create',
                                                headers=qaHeaders, data=json_object)
                logging.info(self.thread_name+':Response Sent for Inserting rows :' + str(insert_idx))
                if create_response.status_code == 200:
                    logging.info(self.thread_name+":Success Rows Inserted:"+str(insert_idx))
                    insert_data_dict["insert"]["rows"] = []
                    with open("thread_status/"+self.thread_name, mode='w') as file:
                        file.write(str(self.thread_start_offset)+":"+str(self.thread_end_offset)+":")
                else:
                    logging.info(self.thread_name+create_response.text)
                    logging.info(self.thread_name+":failed row index:"+str(insert_idx - max_rows_to_insert))
                    logging.info(self.thread_name + str(insert_data_dict['insert']['rows'][0]))
                    sys.exit()
        except:
            traceback.print_exc()

    def validate_domain_sitemap(self, domain_name, row_dict):
        try:
            row_dict['IP Address'] = socket.gethostbyname(domain_name)
            sitemap_url = 'http://' + domain_name + '/sitemap.xml'
            resp = requests.head(sitemap_url, timeout=timeout_seconds)

            if 200 != resp.status_code:
                sitemap_url = 'http://' + domain_name + '/sitemap_index.xml'
                resp = requests.head(sitemap_url, timeout=timeout_seconds)
                if 200 != resp.status_code:
                    row_dict['Active'] = 'No'
                    return

            row_dict['Site Map Url'] = sitemap_url
            row_dict['Active'] = 'Yes'
        except Exception:
            logging.error(self.thread_name+":domain not active:" + domain_name)
            row_dict['Active'] = 'No'


    def populate_row_dict(self, domain, row_dict):
        global validate_domain
        domain_name = "www."+domain+".com"
        row_dict["Domain"] = domain_name
        if "1" == validate_domain:
            self.validate_domain_sitemap(domain_name, row_dict)

    def find_sitemaps(self, domain, insert_data_dict, row_dict, insert_idx):
        self.populate_row_dict(domain, row_dict)
        if insert_idx % max_rows_to_insert == 0:
            self.insert_rows(insert_data_dict, insert_idx)
            time.sleep(20)
        if insert_idx % 100000 == 0:
            logging.info(self.thread_name+'sleeping for 5 minutes')
            time.sleep(300)
        insert_data_dict["insert"]["rows"].append(row_dict.copy())

    def convert_to_csv(self, _row, insert_idx, row_dict, insert_data_dict):
        if insert_idx % max_rows_to_insert == 0:
            self.insert_rows(insert_data_dict, insert_idx)
        row_dict["Domain"] = _row[0]
        insert_data_dict["insert"]["rows"].append(row_dict.copy())

    def __init__(self, thread_name, thread_start_offset, thread_end_offset):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.thread_start_offset = thread_start_offset
        self.thread_end_offset = thread_end_offset

    def run(self):
        logging.info(self.thread_name+":start execution:start offset:"+str(self.thread_start_offset)+":end offset:"
                     +str(self.thread_end_offset))
        domain_set = set()
        insert_data_dict = {"insert": {"rows": []}}
        row_dict = {}
        insert_idx = 1
        with open(input_file_name) as in_file:
            in_file.seek(self.thread_start_offset)
            while self.thread_start_offset <= self.thread_end_offset:
                row = in_file.readline().split(' ')[0]
                if len(row) == 0: break
                if row not in domain_set:
                    domain_set.clear()
                    domain_set.add(row)
                    self.find_sitemaps(row, insert_data_dict, row_dict, insert_idx)
                    insert_idx = insert_idx + 1
                    row_dict = {}
                self.thread_start_offset = in_file.tell()
        self.insert_rows(insert_data_dict,insert_idx)
        logging.info(self.thread_name+": completed")


def delete_rows():
    global qaHeaders
    global qaSitesGridId
    delete_query = '{ "delete": { "query": { "globalFilter": { "filters": ' \
                   '[ { "operator": "LIKE", "keyword": "[^~,]" } ] } } } }'
    response = requests.delete('https://qa.bigparser.com/api/v2/grid/' + qaSitesGridId + '/rows/delete_by_queryObj',
                               headers=qaHeaders, data=delete_query)
    if 200 == response.status_code:
        logging.info("all rows deleted")
    else:
        logging.info("no rows deleted:" + response.text)


if len(sys.argv) < 6 or sys.argv[1] is None or sys.argv[2] is None:
    print("please pass the input file to parse, start index, truncate grid [1/0]"
          ",validate domain sitemap [1/0], no of threads [1..n]. Example python3 poc2.py com.zone 34 1 0 10")
    sys.exit()
input_file_name = sys.argv[1]
start_index = int(sys.argv[2])
truncate_grid = sys.argv[3]
validate_domain = sys.argv[4]
thread_size = int(sys.argv[5])
qaHeaders = {
    'authId': '4b3cc0ec-1619-4f2c-867b-99fac24e3a1d',
    'Content-Type': 'application/json',
}
qaSitesGridId = '6073b4b894a0301ff4b7dfd3'
timeout_seconds = 5
max_rows_to_insert = 2000

def is_retry():
    return len([1 for x in list(os.scandir("thread_status")) if x.is_file()])

try:
    if thread_size == is_retry():
        # write code for using thread from files.
        logging.info("poc2 running in retry mode")
        if "1" == truncate_grid:
            print("please set truncate grid to 0 since the process is running in retry mode")
            sys.exit()
        for t in range(thread_size):
            with open("thread_status/thread-"+str(t)) as f:
                details = f.read().split(":")
                Thread(thread_name="thread-" + str(t), thread_start_offset=int(details[0]),
                   thread_end_offset=int(details[1])).start()
    else:
        if truncate_grid == "1":delete_rows()
        off_file = open('.offset')
        offsets = off_file.read().split(":")
        off_file.close()
        off_len = len(offsets)
        total_offs = off_len - start_index
        logging.info("total line in file:"+input_file_name+":"+str(total_offs))
        offsets_per_thread = round(total_offs / thread_size)

        for t in range(thread_size):
            end_index = start_index + offsets_per_thread
            if end_index >= off_len:
                end_index = off_len -1
            Thread(thread_name="thread-" + str(t), thread_start_offset = int(offsets[start_index]),
                   thread_end_offset = int(offsets[end_index])).start()
            start_index = start_index + offsets_per_thread + 1
except:
    traceback.print_exc()

