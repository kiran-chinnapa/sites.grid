Crawler module spins up multiple spiders to get sitemaps and links for each company urls present 
in the grids defined in crawlers/crawlers/resources/prod_dmv_grids

Before execution make sure the linux server has the following settings for ulimit -n,
number of open files on the server.

ulimit -Hn 500000
ulimit -Sn 500000
fs.file-max = 2097152

reference: 
https://rtcamp.com/tutorials/linux/increase-open-files-limit/
https://www.cyberciti.biz/tips/linux-procfs-file-descriptors.html

******Steps to execute top companies career page extraction into Top_Company_Career_Page.grid******\
cd ~/kiran/crawlers/crawlers/controller \
nohup python3 -u url_extn_spider_controller.py top_companies > top_companies_cp.log 2>&1 & \
logs will be available in top_companies_cp.log

******Steps to execute dmv companies career page extraction into Career_Page.grid******\
cd ~/kiran/crawlers/crawlers/controller \
nohup python3 -u url_extn_spider_controller.py dmv_grids > dmv_grids_cp.log 2>&1 &    
logs will be available in dmv_grids_cp.log

******Steps to execute Job Posting Page extraction******
cd ~/kiran/crawlers/crawlers/controller \
nohup python3 -u job_postings_extractor.py (thread_pool_size) (top_companies|dmv_companies) > job_postings_extractor.log 2>&1 &    
logs will be available in job_postings_extractor.log

******Steps to execute Jobs Extractor Engine******
cd ~/kiran/crawlers/crawlers/controller \
nohup python3 -u job_extractor_engine.py (thread_pool_size) (top_companies|dmv_companies) > job_extractor_engine.log 2>&1 &    
logs will be available in job_extractor_engine.log

******Steps to execute top companies Success Rate Calculator******
cd ~/kiran/crawlers/crawlers/controller \
python3 -u success_rate_calculator.py (top_companies|dmv_companies)    
console output will print the Job Posting page retrieval success rate.


**** Below steps are Deprecated ***\
Steps to execute extraction for Job_Page.grid     
cd ~/kiran/crawlers/crawlers/spiders \
nohup python3 jp_link_extractor.py > jp_link_extractor.log 2>&1 &     
logs will be available in jp_link_extractor.log

Steps to execute extraction for Jobs.grid     
cd ~/kiran/crawlers/crawlers/spiders \
nohup python3 job_desc_extractor.py > job_desc_extractor.log 2>&1 &    
logs will be available in job_desc_extractor.log

Steps to execute top companies career page extraction into Top_Company_Career_Page.grid
cd ~/kiran/crawlers/crawlers/controller \
nohup python3 url_extn_spider_controller.py top_companies > top_companies_cp.log 2>&1 & \
logs will be available in top_companies_cp.log

Steps to execute top companies career page extraction into Top_Company_Career_Page.grid
cd ~/kiran/crawlers/crawlers/controller \
nohup python3 url_extn_spider_controller.py dmv_grids > dmv_grids_cp.log 2>&1 &    
logs will be available in career_page_extractor.log

