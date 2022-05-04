import traceback

from scrapy.spiders import Spider
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from twisted.internet import threads, reactor

from crawlers.spiders.test_spider import TestSpider
from crawlers.utils import keywords, grid_utils


class JPLinkExtractorOld(Spider):
    name = 'jp-link-extractor-old'
    start_urls = []

    # constructor of the class with last two params as non-keyword argument(*args) and keyword argument(**kwargs).
    def __init__(self, root=None, depth=0, *args, **kwargs):
        self.logger.info("[LE] Source: %s Depth: %s Kwargs: %s", root, depth, kwargs)
        self.source = root
        self.options = kwargs
        self.depth = depth
        JPLinkExtractorOld.start_urls.append(root)
        self.le = LinkExtractor(
            allow=self.options.get('allow')
        )
        super(JPLinkExtractorOld, self).__init__(*args, **kwargs)

    def start_requests(self, *args, **kwargs):
        yield Request('%s' % self.source, callback=self.parse)

    def parse(self, response):
        all_urls = []
        if int(response.meta['depth']) <= int(self.depth):
            all_urls = self.get_all_links(response)
            if not all_urls:
                yield {
                    'Job Posting Page': self.source,
                    'Career Page': self.source,
                }
                # print('Job Posting Page: {}, Career Page: {}'.format(self.source, self.source))

            for url in all_urls:
                yield Request('%s' % url, callback=self.parse)
        if len(all_urls) > 0:
            for url in all_urls:
                yield {
                    'Job Posting Page': url,
                    'Career Page': self.source,
                }
                # print('Job Posting Page: {}, Career Page: {}'.format(url, self.source))

    def get_all_links(self, response):
        links = self.le.extract_links(response)
        str_links = []
        for link in links:
            if link.url != self.source:
                str_links.append(link.url)
        return str_links


def process():
    r_settings = get_project_settings()
    r_settings['LOG_LEVEL'] = 'INFO'
    r_settings['ITEM_PIPELINES'] = {
        'crawlers.pipelines.CareerDupDetector': 100,
        'crawlers.pipelines.CareerGridPipeline': 300,
    }
    runner = CrawlerRunner(crawl_settings)
    try:
        comp_urls = grid_utils.get_company_urls()
        for comp_url in comp_urls:
            career_pages = grid_utils.get_all_career_links(comp_url)
            for career_row in career_pages:
                runner.crawl(JPLinkExtractorOld, root=career_row['Career Page URL'], depth=0, allow=keywords.JOBS_PAGES,
                             grid_id=grid_utils.qa_jp_grid_id, unique_column='Job Posting Page')
                # break
            # break
        d = runner.join()
        d.addBoth(lambda _: reactor.stop())
    except:
        traceback.print_exc()


if __name__ == '__main__':
    crawl_settings = get_project_settings()
    crawl_settings['LOG_LEVEL'] = 'INFO'
    # crawl_settings['DOWNLOADER_MIDDLEWARES'] = {
    #    'crawlers.middlewares.JSMiddleware': 543,
    # }
    crawl_settings['ITEM_PIPELINES'] = {
        'crawlers.pipelines.CareerDupDetector': 100,
        'crawlers.pipelines.CareerGridPipeline': 300,
    }
    crawler_runner = CrawlerRunner(crawl_settings)
    try:
        crawler_runner.crawl(TestSpider)
        deferred = threads.deferToThread(process)
        deferred.addCallback(print)
        print('crawlers are deferred to process function')
        reactor.run()  # the script will block here until all crawling jobs are finished
    except:
        traceback.print_exc()

    # debugging code
    # career_pages = [
    #     'https://www.1901group.com/careers/',
    #     'http://2hb.com/html/careers.html',
    #     'https://www.3clogic.com/careers/',
    #     'http://99999consulting.com/careers/',
    #     'https://www.a1logic.com/careers/',
    #     'https://www.cyberds.com/careers',
    #     'https://www.horizonbh.org/careers/',
    #     'http://Humanproof.com/careers',
    #     'https://phe.tbe.taleo.net/phe03/ats/careers/v2/searchResults?org=NAVSTAR&cws=37',
    #     'https://www.obxtek.com/careers',
    # ]

    # crawler_process.crawl(JPLinkExtractorSpider, root=career_pages[2], depth=0, allow='job|career|recruitment', grid_id ='dude')
    # crawler_process.crawl(LinkExtractorSpider, root=career_pages[10], depth=0, allow='435683781')
    # crawler_process.start()
