# sites.grid
Sites.grid Project Repo

poc1.py code uses Federal Contractors Grid, pull all the Sitemaps and #populate Pages.grid
with all URLs on Sitemap and all Metadata (in a diff columns)

poc2.py code reads com.zone 10 gb file and populates sites.grid with all the domain metadata.

poc2_threaded.py does the poc2 functionality in a multi-threaded fashion.

load_sites_grid.py: This module is responsible for parsing single file (com.zone 10 db) and extracting the domain url from each record, validate if the domain exists and populate columns domain url, ip address and active columns into csv file which can be uploaded to bigparser Sites.grid

sites_grid_loader.py: This module is responsible for parsing multiple files and extracting and extracts the following columns, writes the output to big parser sites.grid.
Domain
IP Address
Active
Site Map Url 
Source File

load_pages_grid.py: This module is responsible for parsing all the active-sample csv files, identify the ip address. Using the ip address extract the the following columns into Pages.grid.
Domain
Site Map Url 
Pages
Last Modified

pages_grid_loader.py: This module is responsible for getting site maps from sites.grid based on start row and row count. Using the site map url gets all the pages and inserts the following columns into pages.grid.
Domain
Site Map Url 
Pages
Last Modified
