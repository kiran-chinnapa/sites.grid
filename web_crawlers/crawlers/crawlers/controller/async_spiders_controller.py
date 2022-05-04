import json
import logging
import time
import traceback
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from crawlers.spiders.url_extraction_spider import UrlExtractorSpider
from crawlers.spiders.sitemap_pages_spider import SitemapPagesSpider
from crawlers.utils import grid_utils
from twisted.internet import reactor
from scrapy.utils.log import configure_logging

configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})


# def process_dmv_sm_extraction(_dmv_grids, _good_companies):
#     time.sleep(5)
#     '''sources companies from big parser dmv grids'''
#     start = time.time()
#     try:
#         json_dict = json.loads(grid_utils.query_template)
#         crawl_settings = get_project_settings()
#         # crawl_settings['TELNETCONSOLE_ENABLED'] = False
#         # crawl_settings['FEEDS'] = {
#         #     "company.sitemaps.jl": {"format": "json"},
#         # }
#         # crawl_settings['ITEM_PIPELINES'] = {}
#         logging.info('total number of good companies:{}'.format(len(_good_companies)))
#         for dmv_grid in _dmv_grids:
#             total_row_count = grid_utils.global_search(grid_id=dmv_grid['grid_id'], auth_id=grid_utils.prod_auth_id,
#                                                        search_query=grid_utils.query_count,
#                                                        search_type='search_count',
#                                                        env='prod')
#             logging.info("processing grid:{}:{}".format(dmv_grid['grid_name'], total_row_count))
#             json_dict['query']['selectColumnNames'] = [dmv_grid['company_column'], dmv_grid['website_column'],
#                                                        'Company Name']
#             json_dict['query']['pagination'] = {"startRow": 1, "rowCount": total_row_count['totalRowCount']}
#             result = grid_utils.global_search(grid_id=dmv_grid['grid_id'], auth_id=grid_utils.prod_auth_id,
#                                               search_query=json.dumps(json_dict), env='prod')
#
#             for rslt_rows in result['rows']:
#                 company_website = rslt_rows[dmv_grid['website_column']].strip()
#                 if not company_website.startswith('http'):
#                     company_website = 'http://{}'.format(company_website)
#                 # if not grid_utils.is_row_present('Company Website', company_website, self.grid_id) and not grid_utils.is_row_present(
#                 #         'company_url', company_website, self.uncrawl_grid):
#                 if company_website in _good_companies:
#                     logging.info("processing company:{}".format(company_website))
#                     company_meta = {'company_url': company_website,
#                                     'company_name': rslt_rows['Company Name'],
#                                     'dmv_grid_name': dmv_grid['grid_name'],
#                                     'dmv_grid_id': dmv_grid['grid_id']}
#
#                     runner = CrawlerRunner(settings=crawl_settings)
#                     runner.crawl(SitemapPagesSpider, grid_id=grid_utils.qa_career_grid_id,
#                                  uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
#                                  companies=company_meta)
#
#         d = runner.join()
#         d.addBoth(lambda _: reactor.stop())
#
#     except:
#         traceback.print_exc()
#     logging.info('total time taken by process in seconds {}'.format(time.time() - start))


def process_dmv_url_extraction(_dmv_grids):
    time.sleep(5)
    start = time.time()
    batch_size = 100
    company_cnt = 0
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
        for dmv_grid in _dmv_grids:
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
                if not grid_utils.is_row_present({'Company Website': company_website},
                                                 grid_utils.qa_career_grid_id) and not grid_utils.is_row_present(
                    {'company_url': company_website}, grid_utils.qa_uncrawl_grid_id):
                    logging.info("processing company deferred:{}".format(company_website))
                    company_meta = {'company_url': company_website,
                                    'company_name': rslt_rows['Company Name'],
                                    'dmv_grid_name': dmv_grid['grid_name'],
                                    'dmv_grid_id': dmv_grid['grid_id']}
                    runner.crawl(UrlExtractorSpider, grid_id=grid_utils.qa_career_grid_id,
                                 uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
                                 companies=company_meta)
                    company_cnt = company_cnt + 1
                    if company_cnt % batch_size == 0:
                        logging.info("batch completed processing sleeping for 10 seconds")
                        time.sleep(10)

        d = runner.join()
        d.addBoth(lambda _: reactor.stop())

    except:
        traceback.print_exc()
    logging.info('total time taken by process in seconds {}'.format(time.time() - start))
    logging.info('run clear duplicates job on Career Page grid')


def process_top_companies_url_extraction():
    time.sleep(5)
    try:
        crawl_settings = get_project_settings()
        # crawl_settings['TELNETCONSOLE_ENABLED'] = False
        # crawl_settings['FEEDS'] = {
        #     "company.links.jl": {"format": "json"},
        # }
        # crawl_settings['ITEM_PIPELINES'] = {}
        crawl_settings['DOWNLOADER_MIDDLEWARES'] = {
            'crawlers.middlewares.JSMiddleware': 543,
        }
        runner = CrawlerRunner(settings=crawl_settings)
        with open('../resources/top_companies') as f:
            list_iterable = iter(f.readlines())
            next(list_iterable)
            for line in list_iterable:
                company_meta = {'company_url': line.rstrip('\n') if line.endswith('\n') else line, 'company_name': "",
                                'dmv_grid_name': "", 'dmv_grid_id': ""}
                logging.info('crawling company {}'.format(company_meta['company_url']))
                runner.crawl(UrlExtractorSpider, grid_id=grid_utils.qa_top_companies_grid,
                             uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
                             companies=company_meta)
        d = runner.join()
        d.addBoth(lambda _: reactor.stop())
    except:
        traceback.print_exc()


# def process_top_companies_sm_extraction():
#     time.sleep(5)
#     try:
#         crawl_settings = get_project_settings()
#         # crawl_settings['TELNETCONSOLE_ENABLED'] = False
#         # crawl_settings['FEEDS'] = {
#         #     "company.sitemaps.jl": {"format": "json"},
#         # }
#         # crawl_settings['ITEM_PIPELINES'] = {}
#         runner = CrawlerRunner(settings=crawl_settings)
#         with open('../resources/top_companies') as f:
#             list_iterable = iter(f.readlines())
#             next(list_iterable)
#             for line in list_iterable:
#                 company_meta = {'company_url': line.rstrip('\n') if line.endswith('\n') else line, 'company_name': "",
#                                 'dmv_grid_name': "", 'dmv_grid_id': ""}
#                 logging.info('crawling company {}'.format(company_meta['company_url']))
#                 runner.crawl(SitemapPagesSpider, grid_id=grid_utils.qa_top_companies_grid,
#                              uncrawl_grid=grid_utils.qa_uncrawl_grid_id, unique_column='Career Page',
#                              companies=company_meta)
#         d = runner.join()
#         d.addBoth(lambda _: reactor.stop())
#     except:
#         traceback.print_exc()
