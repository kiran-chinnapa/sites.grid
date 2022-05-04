import logging
import sys
import time
import traceback
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings
from crawlers.controller import async_spiders_controller
from crawlers.spiders.sitemap_pages_spider import SitemapPagesSpider
from crawlers.utils import keywords, grid_utils
import json
from twisted.internet import reactor, threads
from scrapy.utils.log import configure_logging

def read_from_dmv_grids():
    '''sources companies from big parser dmv grids'''
    try:
        json_dict = json.loads(grid_utils.query_template)
        crawl_settings = get_project_settings()
        # crawl_settings['TELNETCONSOLE_ENABLED'] = False
        # crawl_settings['FEEDS'] = {
        #     "company.sitemaps.jl": {"format": "json"},
        # }
        # crawl_settings['ITEM_PIPELINES'] = {}
        dmv_grids = grid_utils.get_all_dmv_grids()
        good_companies = grid_utils.get_company_urls(grid_utils.qa_career_grid_id)
        logging.info('total number of good companies:{}'.format(len(good_companies)))
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
                # if not grid_utils.is_row_present('Company Website', company_website, self.grid_id) and not grid_utils.is_row_present(
                #         'company_url', company_website, self.uncrawl_grid):
                if company_website in good_companies:
                    logging.info("processing company:{}".format(company_website))
                    company_meta = {'company_url': company_website,
                                    'company_name': rslt_rows['Company Name'],
                                    'dmv_grid_name': dmv_grid['grid_name'],
                                    'dmv_grid_id': dmv_grid['grid_id']}

                    runner = CrawlerRunner(settings=crawl_settings)
                    runner.crawl(SitemapPagesSpider, grid_id=grid_utils.qa_career_grid_id,
                                 uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
                                 companies=company_meta)
                    break
            break
        deferred = threads.deferToThread(async_spiders_controller.process_dmv_sm_extraction, dmv_grids, good_companies)
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
        #     "company.sitemaps.jl": {"format": "json"},
        # }
        # crawl_settings['ITEM_PIPELINES'] = {}
        runner = CrawlerRunner(settings=crawl_settings)
        with open('../resources/top_companies') as f:
            for line in f.readlines():
                company_meta = {'company_url': line.rstrip('\n') if line.endswith('\n') else line, 'company_name': "",
                                'dmv_grid_name': "", 'dmv_grid_id': ""}
                logging.info('crawling company {}'.format(company_meta['company_url']))
                runner.crawl(SitemapPagesSpider, grid_id=grid_utils.qa_top_companies_grid,
                             uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
                             companies=company_meta)
                break
        deferred = threads.deferToThread(async_spiders_controller.process_top_companies_sm_extraction)
        deferred.addCallback(print)
        print('crawlers are deferred to process_async function')
        reactor.run()  # the script will block here until all crawling jobs are finished
    except:
        traceback.print_exc()


if __name__ == '__main__':
    start = time.time()
    configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
    if len(sys.argv) != 2:
        print('please pass input args (top_companies, dmv_grids) for spider')
    elif 'top_companies' == sys.argv[1]:
        read_from_top_companies()
    elif 'dmv_grids' == sys.argv[1]:
        read_from_dmv_grids()