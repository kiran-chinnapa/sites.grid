import json
import logging
import sys
import time
import traceback
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
from crawlers.controller import async_spiders_controller
from crawlers.spiders.url_extraction_spider import UrlExtractorSpider
from crawlers.utils import keywords, grid_utils
from twisted.internet import reactor, threads
from scrapy.utils.log import configure_logging


def read_from_dmv_grids():
    start = time.time()
    try:
        json_dict = json.loads(grid_utils.query_template)
        crawl_settings = get_project_settings()
        crawl_settings['DOWNLOADER_MIDDLEWARES'] = {
            'crawlers.middlewares.JSMiddleware': 543,
        }
        crawl_settings['TELNETCONSOLE_ENABLED'] = False
        # crawl_settings['FEEDS'] = {
        #    "company.links.jl": {"format": "json"},
        # }
        # crawl_settings['ITEM_PIPELINES'] = {}
        runner = CrawlerRunner(settings=crawl_settings)
        dmv_grids = grid_utils.get_all_dmv_grids()
        for dmv_grid in dmv_grids:
            total_row_count = grid_utils.global_search(grid_id=dmv_grid['grid_id'], auth_id=grid_utils.prod_auth_id,
                                                       search_query=grid_utils.query_count,
                                                       search_type='search_count',
                                                       env='prod')
            logging.info("processing grid:{}:{}".format(dmv_grid['grid_name'], total_row_count))
            json_dict['query']['selectColumnNames'] = [dmv_grid['company_column'], dmv_grid['website_column'],
                                                       'Company Name']
            json_dict['query']['pagination'] = {"startRow": 1, "rowCount": total_row_count['totalRowCount']}
            result = grid_utils.global_search(grid_id=dmv_grid['grid_id'], auth_id=grid_utils.prod_auth_id,
                                              search_query=json.dumps(json_dict), env='prod')

            for rslt_rows in result['rows']:
                company_website = rslt_rows[dmv_grid['website_column']].strip()
                if not company_website.startswith('http'):
                    company_website = 'http://{}'.format(company_website)
                    # if not grid_utils.is_row_present('Company Website', company_website, grid_utils.qa_career_grid_id) and not grid_utils.is_row_present(
                    #         'company_url', company_website, grid_utils.qa_uncrawl_grid_id):
                logging.info("processing company:{}".format(company_website))
                company_meta = {'company_url': company_website,
                                'company_name': rslt_rows['Company Name'],
                                'dmv_grid_name': dmv_grid['grid_name'],
                                'dmv_grid_id': dmv_grid['grid_id']}
                runner.crawl(UrlExtractorSpider, grid_id=grid_utils.qa_career_grid_id,
                             uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
                             companies=company_meta)
                break
            break
        deferred = threads.deferToThread(async_spiders_controller.process_dmv_url_extraction, dmv_grids)
        deferred.addCallback(print)
        print('crawlers are deferred to process_async function')
        reactor.run()  # the script will block here until all crawling jobs are finished

    except:
        traceback.print_exc()
    logging.info('total time taken by process in seconds {}'.format(time.time() - start))


def read_from_top_companies():
    try:
        crawl_settings = get_project_settings()
        # crawl_settings['TELNETCONSOLE_ENABLED'] = False
        # crawl_settings['FEEDS'] = {
        #     "company.links.jl": {"format": "json"},
        # }
        # crawl_settings['ITEM_PIPELINES'] = {}
        runner = CrawlerRunner(settings=crawl_settings)
        company_meta = {'company_url': "",
                        'company_name': "",
                        'dmv_grid_name': "",
                        'dmv_grid_id': ""}
        with open('../resources/top_companies') as f:
            for line in f.readlines():
                company_meta['company_url'] = line.rstrip('\n') if line.endswith('\n') else line
                logging.info('crawling company {}'.format(company_meta['company_url']))
                runner.crawl(UrlExtractorSpider, grid_id=grid_utils.qa_top_companies_grid,
                             uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
                             companies=company_meta)
                break
        deferred = threads.deferToThread(async_spiders_controller.process_top_companies_url_extraction)
        deferred.addCallback(print)
        print('crawlers are deferred to process_async function')
        reactor.run()  # the script will block here until all crawling jobs are finished
    except:
        traceback.print_exc()


configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('please pass input args (top_companies, dmv_grids) for spider')
    elif 'top_companies' == sys.argv[1]:
        read_from_top_companies()
    elif 'dmv_grids' == sys.argv[1]:
        read_from_dmv_grids()
