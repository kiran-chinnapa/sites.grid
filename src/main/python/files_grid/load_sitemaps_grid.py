
import time
import json
import sys
import csv
import decode_sitemap_url
import grid_utils

startTime = time.time()
max_rows_to_insert = 500
grid_id = '60859f7e94a030121048376b'
auth_id = 'b1427e2f-81f4-4280-b56b-305ba8f36b5c'
insertDataDict = {"insert": {"rows": [{}]}}
rowDict = {}
insert_idx = 1

delete_json_str = '{ "delete": { "query": { "globalFilter": { "filters": ' \
               '[ { "operator": "LIKE", "keyword": "www." } ] } } } }'
grid_utils.truncate(grid_id, auth_id, delete_json_str)

with open(sys.argv[1]) as read_obj:
    csv_reader = csv.reader(read_obj)
    for csv_row in csv_reader:
        if "Yes" == csv_row[2]:
            sitemap_url = decode_sitemap_url.get_sitemap_url(csv_row[0])
            if sitemap_url is None:
                continue
            rowDict["Domain"] = csv_row[0]
            rowDict["Site Map Url"] = sitemap_url
            insertDataDict["insert"]["rows"].append(rowDict.copy())
            if insert_idx % max_rows_to_insert == 0:
                json_object = json.dumps(insertDataDict, indent=4)
                grid_utils.add_row(grid_id, auth_id, json_object)
                insertDataDict["insert"]["rows"] = []
            insert_idx = insert_idx + 1
            rowDict = {}

json_object = json.dumps(insertDataDict, indent=4)
grid_utils.add_row(grid_id, auth_id, json_object)
endTime = time.time()
elapsed = endTime - startTime
print("total number of rows inserted:" + str(insert_idx) + ":time elapsed seconds:" + str(elapsed))
