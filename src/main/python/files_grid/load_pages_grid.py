# This module is responsible for parsing all the active-sample csv files, identify the ip address.
# Using the ip address extract the following columns to construct page record.
#
# Domain : extracted from ip address
# Site Map Url : extracted from robots.txt file
# Pages : extracted by traversing sitemap xml
# Last Modified : extracted by traversing sitemap xml
#
# The page record is transmitted through QA big parser grid through API '/rows/bulk_create'
# This module is single threaded uses pandas and beautiful soup framework for web crawling.
# Total 10 k domains and around 6 million pages records are transmitted to big parser.
# TPS is 3 million pages in 12 hours with single thread.

import logging
import os
import time
import sys
import csv
import counters
import decode_sitemap_url
import grid_utils
import metadata
import panda_load_pages
import glob
import urllib3

if len(sys.argv) != 2:
    print('''please pass csv domains file path to proceed. 
    Example:
    python3 load_pages_grid.py "/home/ubuntu/kiran/active-sample/*.csv"''')
    sys.exit(0)

class LoadPagesGrid:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    startTime = time.time()
    max_rows_to_insert = 2000
    # original grid
    # grid_id = '6084301494a030356bf98c12'
    # debug grid
    grid_id = '60fd25fb94a030695443b082'
    auth_id = 'b1427e2f-81f4-4280-b56b-305ba8f36b5c'
    insertDataDict = {"insert": {"rows": []}}
    rowDict = {}
    sm_obj = decode_sitemap_url.DecodeSiteMapUrl()
    metata_obj = metadata.Metadata()
    panda_pages = panda_load_pages.PandasLoadPages()

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='load_pages_grid.log')

    delete_json_str = '{ "delete": { "query": { "globalFilter": { "filters": ' \
                   '[ { "operator": "LIKE", "keyword": "[a-zA-Z0-9_]" } ] } } } }'
    if not os.path.exists(os.getcwd() + "/.completed"):
        grid_utils.GridUtils.truncate(grid_id, auth_id, delete_json_str)

    with open(".completed", "r") as comp_file_read:
        comp_list = comp_file_read.read().split("\n")
    for file in glob.glob(sys.argv[1]):
        if file in comp_list:
            continue
        logging.info("processing file: "+file)
        with open(file) as read_obj:
            csv_reader = csv.reader(read_obj)
            for csv_row in csv_reader:
                if len(csv_row) < 3:
                    logging.info("no ip address in the file, skipping....")
                    continue
                logging.info("processing domain: "+ csv_row[0])
                resp_url = metata_obj.get_resp_url(csv_row[0])
                if metata_obj.get_language(resp_url.resp) not in ['english', 'en', 'en-us']:
                    logging.info("non english domain, skipping....")
                    continue

                sitemap_url = sm_obj.get_sitemap_url(resp_url.url)

                if sitemap_url is None:
                    logging.info('no sitemap for domain:' + csv_row[0])
                    continue

                try:
                    dataframe = panda_pages.parse_sitemap(sitemap_url, ["loc", "lastmod"], grid_id, auth_id, max_rows_to_insert, resp_url)
                    logging.info("returned dataframe")
                except:
                    logging.info('no pages for sitemap url:' + sitemap_url)
                    continue
                if dataframe.empty:
                    logging.info('no pages for sitemap url:'+ sitemap_url)
                    continue

                logging.info("row count of data-frame:{}".format(dataframe.shape[0]))
                domain_pages = panda_pages.get_all_page_links(resp_url.url)
                for index, row in dataframe.iterrows():
                    # logging.info("iterating dataframe")
                    if row['loc'] is None or row['loc'].strip() == '':
                        continue
                    if row['loc'].startswith('http'):
                        pg = row['loc']
                    else:
                        pg = '{}{}{}'.format(resp_url.pcl, resp_url.domain, row['loc'])
                    rowDict["Domain"] = resp_url.domain
                    rowDict["Site Map Url"] = sitemap_url
                    rowDict["Pages"] = pg
                    rowDict["Last Modified"] = row['lastmod']
                    insertDataDict["insert"]["rows"].append(rowDict.copy())
                    counters.set_insert_idx_size(1)
                    if counters.insert_idx % max_rows_to_insert == 0:
                        grid_utils.GridUtils.add_row(grid_id, auth_id, insertDataDict, len(insertDataDict["insert"]["rows"]))
                        insertDataDict["insert"]["rows"] = []
                    rowDict = {}
                    if row['loc'] in domain_pages:
                        domain_pages.remove(row['loc'])
                if len(insertDataDict["insert"]["rows"]) > 0:
                    grid_utils.GridUtils.add_row(grid_id, auth_id, insertDataDict,
                                                 len(insertDataDict["insert"]["rows"]))
                    insertDataDict["insert"]["rows"] = []
                del dataframe
                rowDict = {}
                for pg in domain_pages:
                    if pg is None or pg.strip() == '':
                        continue
                    rowDict["Domain"] = resp_url.domain
                    rowDict["Site Map Url"] = sitemap_url
                    rowDict["Pages"] = pg
                    rowDict["Last Modified"] = ''
                    insertDataDict["insert"]["rows"].append(rowDict.copy())
                    counters.set_insert_idx_size(1)
                    if counters.insert_idx % max_rows_to_insert == 0:
                        grid_utils.GridUtils.add_row(grid_id, auth_id, insertDataDict, len(insertDataDict["insert"]["rows"]))
                        insertDataDict["insert"]["rows"] = []
                    rowDict = {}
                if len(insertDataDict["insert"]["rows"]) > 0:
                    grid_utils.GridUtils.add_row(grid_id, auth_id, insertDataDict,
                                                 len(insertDataDict["insert"]["rows"]))
                    insertDataDict["insert"]["rows"] = []

        logging.info("marking {} as completed file".format(file))
        with open(".completed", "a") as comp_file_write:
            comp_file_write.write(file + "\n")

        logging.info("total bandwidth used in bytes sent:{} received:{} insert count:{}".format(
            counters.total_req_size, counters.total_resp_size,counters.insert_idx))
        if None != os.getenv("max_insert_limit") and counters.insert_idx > int(os.getenv("max_insert_limit")):
            logging.info("insert limit reached")
            break

    # if len(insertDataDict["insert"]["rows"]) > 0:
    #     grid_utils.GridUtils.add_row(grid_id, auth_id, insertDataDict, len(insertDataDict["insert"]["rows"]))
    #     insertDataDict["insert"]["rows"] = []
    endTime = time.time()
    elapsed = endTime - startTime
    logging.info("process completed, time elapsed in seconds:" + str(elapsed))
    logging.info("total bandwidth used by process in bytes sent:{} received:{} insert_count:{} seconds_elapsed: {}".format(
        counters.total_req_size,counters.total_resp_size,counters.insert_idx,elapsed))
