# The code below reads the 10 gb file and populates sites.grid.
# please pass the input file to parse, start index, truncate grid [1/0],
# validate domain sitemap [1/0]. Example python3 poc2.py com.zone 34 1 0

import csv
import json
import logging
import sys
import socket
import time

import requests

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='poc2.log')

insertDataDict = {"insert": {"rows": []}}
rowDict = {}
domain_set = set()
insert_idx = 1
if len(sys.argv) < 5 or sys.argv[1] is None or sys.argv[2] is None:
    print("please pass the input file to parse, start index, truncate grid [1/0]"
          ",validate domain sitemap [1/0]. Example python3 poc2.py com.zone 34 1 0")
    sys.exit()
input_file_name = sys.argv[1]
start_index = sys.argv[2]
truncate_grid = sys.argv[3]
validate_domain = sys.argv[4]

qaHeaders = {
    'authId': '4b3cc0ec-1619-4f2c-867b-99fac24e3a1d',
    'Content-Type': 'application/json',
}

qaSitesGridId = '6073b4b894a0301ff4b7dfd3'

timeout_seconds = 5
max_rows_to_insert = 500


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


def insert_rows():
    json_object = json.dumps(insertDataDict, indent=4)
    if bool(insertDataDict['insert']['rows']):
        # print("***************** insert ***************")
        # print(json_object)
        logging.info('Request Sent for Inserting rows :'+str(insert_idx))
        create_response = requests.post('https://qa.bigparser.com/api/v2/grid/' + qaSitesGridId + '/rows/create',
                                        headers=qaHeaders, data=json_object)
        logging.info('Response Sent for Inserting rows :' + str(insert_idx))
        if create_response.status_code == 200:
            logging.info("Success Rows Inserted:"+str(insert_idx))
            insertDataDict["insert"]["rows"] = []
        else:
            logging.info(create_response.text)
            logging.info("failed row index:"+str(insert_idx - max_rows_to_insert))
            logging.info(insertDataDict['insert']['rows'][0])
            sys.exit()


def validate_domain_sitemap(domain_name):
    try:
        sitemap_url = 'http://' + domain_name + '/sitemap.xml'
        resp = requests.head(sitemap_url, timeout=timeout_seconds)

        if 200 != resp.status_code:
            sitemap_url = 'http://' + domain_name + '/sitemap_index.xml'
            resp = requests.head(sitemap_url, timeout=timeout_seconds)
            if 200 != resp.status_code:
                rowDict['Active'] = 'No'
                return

        rowDict['Site Map Url'] = sitemap_url
        rowDict['IP Address'] = socket.gethostbyname(domain_name)
        rowDict['Active'] = 'Yes'
    except Exception:
        logging.error("site map not available for domain:" + domain_name)
        rowDict['Active'] = 'No'


def populate_row_dict(domain):
    global rowDict, validate_domain
    domain_name = "www."+domain+".com"
    rowDict["Domain"] = domain_name
    if "1" == validate_domain:
        validate_domain_sitemap(domain_name)


def find_sitemaps(domain):
    populate_row_dict(domain)

    global insert_idx, rowDict, insertDataDict
    if insert_idx % max_rows_to_insert == 0:
        insert_rows()
        time.sleep(20)
    insertDataDict["insert"]["rows"].append(rowDict.copy())
    insert_idx = insert_idx + 1
    rowDict = {}


def convert_to_csv(_row):
    global insert_idx, rowDict, insertDataDict
    if insert_idx % max_rows_to_insert == 0:
        insert_rows()
    rowDict["Domain"] = _row[0]
    insertDataDict["insert"]["rows"].append(rowDict.copy())
    insert_idx = insert_idx + 1
    rowDict = {}


def process_site_maps(_row):
    if row and index > 34 and row[0] not in domain_set:
        domain_set.clear()
        domain_set.add(_row[0])
        find_sitemaps(_row[0])


if truncate_grid == "1":
    delete_rows()
with open(input_file_name, "r") as read_obj:
    csv_reader = csv.reader(read_obj, delimiter=' ')
    index = 0
    for row in csv_reader:
        if index > int(start_index):
            process_site_maps(row)
        index = index + 1
insert_rows()
