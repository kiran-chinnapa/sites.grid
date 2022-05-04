# The code below uses Federal Contractors Grid, pull all the Sitemaps and #populate Pages.grid
# with all URLs on Sitemap and all Metadata (in a diff columns)

import time
import json
import requests
import xmltodict

qaHeaders = {
            'authId': 'b1427e2f-81f4-4280-b56b-305ba8f36b5c',
            'Content-Type': 'application/json',
        }
qaPagesGridId = '6065739094a030381abf0cfd'

startTime = time.time()
timeout_seconds = 5
max_rows_to_insert = 499
insertDataDict = {"insert": {"rows": [{}]}}
rowDict = {}
insert_idx = 0

metadata_set = set()

class POC1_OO:

    def delete(self):

        delete_query = '{ "delete": { "query": { "globalFilter": { "filters": ' \
                       '[ { "operator": "LIKE", "keyword": "http" } ] } } } }'

        response = requests.delete('https://qa.bigparser.com/api/v2/grid/' + qaPagesGridId + '/rows/delete_by_queryObj',
                                   headers=qaHeaders, data=delete_query)
        if 200 == response.status_code:
            print("all rows deleted")
        else:
            print("no rows deleted:" + response.text)


    def get_site_map_dict(self,url_param, path):
        site_map_dict = {}
        site_map_url = url_param + path
        try:
            resp = requests.get(site_map_url, timeout=timeout_seconds)
            if 200 != resp.status_code:
                print("site map not available for:" + url_param)
            site_map_dict = xmltodict.parse(resp.text)
        except:
            print("site map not available for:" + url_param)
        return site_map_dict


    def get_pages_from_site_map(self,url_param):
        site_map_dict = self.get_site_map_dict(url_param, "sitemap.xml")

        if not bool(site_map_dict):
            site_map_dict = self.get_site_map_dict(url_param, "sitemap_index.xml")
            if not bool(site_map_dict):
                print("site map not available for:" + url_param)
                return

        if 'urlset' in site_map_dict.keys():
            return site_map_dict['urlset']['url']
        elif 'sitemap' in site_map_dict.keys():
            return site_map_dict['sitemap']['url']
        elif 'sitemapindex' in site_map_dict.keys():
            return site_map_dict['sitemapindex']['sitemap']
        else:
            print("unknown key for sitemap:" + url_param)
            return {}

    def search(self):
        fdaGridViewId = '5a81eb784463b73e3c254221/share/5c322d71c9d08247321fbeac'
        headers = {
            'authId': '62aa6b07-b558-4bf7-86fa-63d6ae623b24',
            'Content-Type': 'application/json',
        }

        data = '{ "query": { "columnFilter": { "filters": ' \
               '[ { "column": "Website URL", "operator": "NEQ", "keyword": null } ] },' \
               ' "selectColumnNames": [ "Website URL" ] } }'

        response = requests.post('https://www.bigparser.com/api/v2/grid/' + fdaGridViewId + '/search',
                                 headers=headers, data=data)

        json_dict = json.loads(response.text)

        urlList = json_dict['rows']
        return urlList

    def insert_rows(self):
        json_object = json.dumps(insertDataDict, indent=4)
        insertDataDict["insert"]["rows"] = []
        # print(json_object)
        # file = open("json_insert","w")
        # file.write(json_object)
        create_response = requests.post('https://qa.bigparser.com/api/v2/grid/' + qaPagesGridId + '/rows/create',
                                        headers=qaHeaders, data=json_object)
        if create_response.status_code == 200:
            print("Success Rows Inserted")
        else:
            print(create_response.text)




# def buildDataString(url,page):
#     global insertData
#     insertData = insertData + ''
#     print(insertData)


obj = POC1_OO()
urlList = obj.search()

for url in range(len(urlList)):
    print("finding sitemaps for :" + urlList[url][0])
    # if("https://www.thinkgeek.com/" == urlList[url][0]):
    #     getPagesFromSiteMap(urlList[url][0])
    siteMapDict = obj.get_pages_from_site_map(urlList[url][0])
    if bool(siteMapDict):
        # insertData = '{ "insert": { "rows": ['
        for siteMapIdx in range(len(siteMapDict)):
            if insert_idx % max_rows_to_insert == 0:
                obj.insert_rows()
            rowDict["FC Web URL"] = urlList[url][0]
            rowDict["SiteMap Pages"] = siteMapDict[siteMapIdx]['loc']
            if 'changefreq' in siteMapDict[siteMapIdx]:
                rowDict["changefreq"] = siteMapDict[siteMapIdx]['changefreq']
            if 'lastmod' in siteMapDict[siteMapIdx]:
                rowDict["lastmod"] = siteMapDict[siteMapIdx]['lastmod']
            if 'priority' in siteMapDict[siteMapIdx]:
                rowDict["priority"] = siteMapDict[siteMapIdx]['priority']
            # print(siteMapDict[siteMapIdx].keys())
            # for key in siteMapDict[siteMapIdx].keys():
            #     metadata_set.add(key)
            insertDataDict["insert"]["rows"].append(rowDict.copy())
            insert_idx = insert_idx + 1
            rowDict = {}
obj.insert_rows()
endTime = time.time()
elapsed =  endTime - startTime
print("total number of rows inserted:" + str(insert_idx) + ":time elapsed seconds:" + str(elapsed))
# print(metadata_set)
