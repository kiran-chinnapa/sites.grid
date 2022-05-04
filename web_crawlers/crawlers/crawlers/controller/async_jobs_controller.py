"""1. Loop through all the DMV grids :grid_utils.global_search
2. For each row extract the company name and website: grid_utils.read_dmv_companies()
3. Using career page regex use sitemap spider and link extraction spider to find all career pages for given company."""

import csv
import json
import logging
import traceback
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from crawlers.spiders.sitemap_pages_spider import SitemapPagesSpider
from crawlers.spiders.url_extraction_spider import UrlExtractorSpider
from crawlers.utils import grid_utils, keywords
import time
from twisted.internet import reactor


def process_batch(runner):
    SitemapPagesSpider.sitemap_urls = robots_urls.copy()
    runner.crawl(SitemapPagesSpider)
    for root_url in company_urls:
        runner.crawl(UrlExtractorSpider, root=root_url, depth=0, allow=keywords.CAREER_PAGE, deny=keywords.CAREER_PAGE_DENY)


batch_size = 100
robots_urls, company_urls = [], []
configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
json_dict = json.loads(grid_utils.query_template)
start = time.time()


def process():
    company_cnt = 0
    try:
        crawl_settings = get_project_settings()
        runner = CrawlerRunner(settings=crawl_settings)
        with open('../resources/prod_dmv_grids') as file:
            csv_reader = csv.reader(file)
            next(csv_reader, None)
            for row in csv_reader:
                logging.info("processing grid:{}".format(row[0]))
                total_row_count = grid_utils.global_search(grid_id=row[0], auth_id=grid_utils.prod_auth_id,
                                                           search_query=grid_utils.query_count,
                                                           search_type='search_count',
                                                           env='prod')
                logging.info(total_row_count)
                json_dict['query']['selectColumnNames'] = [row[1], row[2]]
                json_dict['query']['pagination'] = {"startRow": 1, "rowCount": total_row_count['totalRowCount']}
                result = grid_utils.global_search(grid_id=row[0], auth_id=grid_utils.prod_auth_id,
                                                  search_query=json.dumps(json_dict), env='prod')

                for rslt_rows in result['rows']:
                    company_website = rslt_rows[row[2]].strip()
                    if not company_website.startswith('http'):
                        company_website = 'http://{}'.format(company_website)

                    robots_urls.append('{}{}'.format(company_website, '/robots.txt'))
                    company_urls.append(company_website)
                    company_cnt = company_cnt + 1

                    if company_cnt % batch_size == 0:
                        process_batch(runner)
                        logging.info("batch completed processing sleeping for 10 seconds")
                        time.sleep(10)
                        robots_urls.clear()
                        company_urls.clear()

        logging.info("total companies count : {}".format(company_cnt))

        if company_cnt % batch_size > 0:
            process_batch(runner)
            logging.info("last batch completed processing")
            robots_urls.clear()
            company_urls.clear()

        d = runner.join()
        d.addBoth(lambda _: reactor.stop())

    except:
        traceback.print_exc()
    logging.info('total time taken by process in seconds {}'.format(time.time() - start))
