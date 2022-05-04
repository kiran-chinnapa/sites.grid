"""1. Loop through all the DMV grids :grid_utils.global_search
2. For each row extract the company name and website: grid_utils.read_dmv_companies()
3. Using career page regex use sitmap spider and link extraction spider to find all career pages for given company."""

import csv
import json
import logging
import traceback
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from crawlers.controller import async_jobs_controller
from crawlers.spiders.sitemap_pages_spider import SitemapPagesSpider
from crawlers.spiders.url_extraction_spider import UrlExtractorSpider
from crawlers.utils import grid_utils, keywords
import time
from twisted.internet import reactor, threads

auth_id = 'ed0444ad-9e34-43b5-b0f5-5d08351a3def'
query_template = '{ "query": { "selectColumnNames": [], ' \
                 '"pagination": {},' \
                 '"sendRowIdsInResponse": true, "showColumnNamesInResponse": true}}'

configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
json_dict = json.loads(query_template)
start = time.time()
d = None
try:
    crawl_settings = get_project_settings()
    runner = CrawlerRunner(settings=crawl_settings)
    with open('../resources/prod_dmv_grids') as file:
        csv_reader = csv.reader(file)
        next(csv_reader, None)
        for row in csv_reader:
            logging.info("processing grid:{}".format(row[0]))
            json_dict['query']['selectColumnNames'] = [row[1], row[2]]
            json_dict['query']['pagination'] = {"startRow": 1, "rowCount": 1}
            result = grid_utils.global_search(grid_id=row[0], auth_id=auth_id,
                                              search_query=json.dumps(json_dict), env='prod')

            for rslt_rows in result['rows']:
                company_website = rslt_rows[row[2]].strip()
                if not company_website.startswith('http'):
                    company_website = 'http://{}'.format(company_website)

                SitemapPagesSpider.sitemap_urls = ['{}{}'.format(company_website, '/robots.txt')]
                runner.crawl(SitemapPagesSpider)
                runner.crawl(UrlExtractorSpider, root=company_website, depth=0, allow=keywords.CAREER_PAGE, deny=keywords.CAREER_PAGE_DENY)
            break

    deferred = threads.deferToThread(async_jobs_controller.process)
    deferred.addCallback(print)
    print('crawlers are deferred to async_jobs_controller')
    reactor.run()  # the script will block here until all crawling jobs are finished

except:
    traceback.print_exc()
logging.info('total time taken by process in seconds {}'.format(time.time() - start))
