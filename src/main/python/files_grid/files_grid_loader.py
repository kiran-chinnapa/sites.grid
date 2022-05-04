# crawl all pages from pages.grid for files
# categorise the files, example resume etc.
# resume categorization is still not hundred percent accurate.
import datetime
import logging
import os
import re
import threading
import traceback
import counters
import grid_utils
import urllib3
import metadata
import regex_all


class MyThread(threading.Thread):
    def __init__(self, start_row, row_count):
        threading.Thread.__init__(self)
        self.start_row = start_row
        self.row_count = row_count
        # pdf, xls, xlsx, doc, docx, rtf, ppt, pptx, csv, txt, xml, json
        self.query = "{\"query\":{\"columnFilter\":{\"filters\":[{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".pdf\"}," \
                     "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".doc\"},{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".docx\"}," \
                     "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".xls\"},{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".xlsx\"}," \
                     "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".rtf\"},{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".ppt\"}," \
                     "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".xml\"},{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".zip\"}]}," \
                     "\"selectColumnNames\":[\"Pages\"]," \
                     "\"pagination\":{\"startRow\":1,\"rowCount\":1000},\"sendRowIdsInResponse\":true}}"

        # self.query = "{\"query\":{\"columnFilter\":{\"filters\":[{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".pdf\"}," \
        #              "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".doc\"},{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".docx\"}," \
        #              "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".xls\"},{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".xlsx\"}," \
        #              "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".ppt\"}," \
        #              "{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".xml\"}]},\"selectColumnNames\":[\"Pages\"]," \
        #              "\"pagination\":{\"startRow\":1,\"rowCount\":1000},\"sendRowIdsInResponse\":true}}"

        # self.query= "{\"query\":{\"columnFilter\":{\"filters\":[{\"column\":\"Pages\",\"operator\":\"LIKE\",\"keyword\":\".pdf\"}]},\"selectColumnNames\":[\"Pages\"],\"pagination\":{\"startRow\":1,\"rowCount\":10},\"sendRowIdsInResponse\":true}}"
        # self.query = "{\"query\":{\"globalFilter\":{\"filters\":[{\"operator\":\"LIKE\",\"keyword\":\"[A-Za-z0-9]\"}]}}}"
        self.rowDict = {}
        self.insert_idx = 0
        self.insertDataDict = {"insert": {"rows": []}}
        self.resume_pattern = re.compile(regex_all.resume,re.IGNORECASE)
        self.metadata = metadata.Metadata(self.resume_pattern)


    def insert_to_resumes_grid(self, s_rows):
        try:
            for s_row in s_rows:
                logging.info(str(threading.get_ident())+":processing page:"+s_row[1])
                logging.info("resume found:"+s_row[1])
                # print ("resume found:"+s_row[1])
                # sys.exit()
                # add to resume.grid
                self.rowDict["File Name"] = self.metadata.get_file_name(s_row[1])
                self.rowDict["File Extension"] = self.metadata.get_file_extension(s_row[1])
                self.rowDict["Date & time Stamp"] = datetime.datetime.now().strftime("%H:%M:%S %d/%m/%Y")
                self.rowDict["File Creation Date"] = "under implementation"
                self.rowDict["File Modification Date"] = "under implementation"
                self.rowDict["Source URL"] = s_row[1]
                # self.rowDict["Category"] = self.metadata.get_file_category(s_row[1])
                self.rowDict["No. of Pages"] = "under implementation"
                b_i_u_dict = self.metadata.get_bold_italics_texts(s_row[1])
                self.rowDict["Bold Text"] = b_i_u_dict['bold']
                self.rowDict["Italics Text"] = b_i_u_dict['italics']
                self.rowDict["Underlined Text"] = b_i_u_dict['underlined']
                text = self.metadata.get_text_from_file_url(s_row[1])
                if '' != text :
                    self.rowDict["Tech stack"] = self.metadata.get_file_tech_stack(text)
                    self.rowDict["Phone Number"] = self.metadata.get_primary_phone(text)
                    self.rowDict["Email"] = self.metadata.get_primary_email(text)
                    self.rowDict["Total Experience"] = self.metadata.get_file_total_experience(text)
                    self.rowDict["Keywords"] = self.metadata.get_all_keywords(text)
                    self.rowDict["Capitalized Keywords"] = self.metadata.get_all_capitalized_keywords(text)

                self.rowDict["File Description"] = "under implementation"
                self.rowDict["No. of Headings"] = "under implementation"
                self.rowDict["No. of Sub Headings"] = "under implementation"
                self.rowDict["No. of Tables"] = "under implementation"
                self.rowDict["Font Size Ratios"] = "under implementation"
                self.rowDict["No. of Images"] = "under implementation"
                self.insertDataDict["insert"]["rows"].append(self.rowDict.copy())
                self.insert_idx = self.insert_idx + 1
                if self.insert_idx % max_rows_to_insert == 0:
                    grid_utils.GridUtils.add_row(files_grid_id,auth_id,self.insertDataDict,self.insert_idx)
                    self.insertDataDict["insert"]["rows"] = []
                self.rowDict = {}
            if len(self.insertDataDict["insert"]["rows"]) > 0:
                grid_utils.GridUtils.add_row(files_grid_id,auth_id,self.insertDataDict,self.insert_idx)
        except Exception as e:
            traceback.print_exc()
            logging.error("error in insert_to_resume_grid: {}".format(e))



    def run(self):
        try:
            logging.info(str(threading.get_ident()) + ":thread processing start row:" + self.start_row)
            json_rslt = grid_utils.GridUtils.search(pages_grid_id, auth_id, self.query, "search")
            #print("start:count->" + self.start_row + ":" + self.row_count)
            # print(json_rslt['rows'])
            if os.getenv('debug_flag') == '1':
                json_rslt['rows'] = [['612788f394a0300b79f8d518', 'https://coreyms.com/portfolio/docs/Corey-Schafer-Resume.pdf']]
            if None != json_rslt:
                self.insert_to_resumes_grid(json_rslt['rows'])
            logging.info(str(threading.get_ident()) + ":thread completed :" + self.start_row + ": inserted:" + str(self.insert_idx))
            # pages_comp_file.write(self.start_row + ",")
        except:
            traceback.print_exc()


pages_grid_id = "6084301494a030356bf98c12"
auth_id = "b1427e2f-81f4-4280-b56b-305ba8f36b5c"
max_rows_to_insert = 10
if os.getenv("debug_flag") == "1":
    max_rows_to_insert =1
files_grid_id = "6127b75594a030139f194409"

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='files_grid_loader.log')

# delete_json_str = '{ "delete": { "query": { "globalFilter": { "filters": ' \
#                        '[ { "operator": "LIKE", "keyword": "[a-zA-Z0-9_]" } ] } } } }'
# if not os.path.exists(os.getcwd()+"/.pages_completed"):
#     grid_utils.GridUtils.truncate(pages_grid_id, auth_id, delete_json_str)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
thread_size = 1
threads = []
s_row = 1
r_count = 1000
# get total row count


# t_row_cnt_query = "{\"query\":{\"columnFilter\":{\"filters\":[{\"column\":\"Active\",\"operator\":\"EQ\",\"keyword\":\"Yes\"}]}}}"
total_row_cnt = 3142
i = 0
# for every n threads do join, do it upto total row count
# with open(".pages_completed", "r+") as pages_comp_file:
#     lst = pages_comp_file.read().split(",")
for st_row in range(s_row, total_row_cnt, r_count):
    i = i + 1
    # if str(st_row) in lst:
    #     continue
    if i % thread_size == 0:
        for t in threads:
            t.join()
        threads = []
    t = MyThread(str(st_row), str(r_count))
    t.start()
    threads.append(t)
for t in threads:
    t.join()

logging.info("total bandwidth used in bytes sent:{} received:{}".format(counters.total_req_size,counters.total_resp_size))




