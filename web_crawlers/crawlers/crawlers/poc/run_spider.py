import time

from scrapy import signals
from scrapy.crawler import Crawler, CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, threads

from crawlers.poc import run_spider_thread
from crawlers.spiders.scrapyloop import ScrapyLoop
from crawlers.spiders.sitemap_pages_spider import SitemapPagesSpider
from crawlers.spiders.url_extraction_spider import UrlExtractor
from crawlers.utils import keywords


# ScrapyLoop(success_interval=10).loop_crawl(SitemapPagesSpider,queue_count_func=get_count)
# crawler = Crawler(SitemapPagesSpider, get_project_settings())
# crawler.signals.connect(SitemapPagesSpider.close, signals.spider_closed)
# ScrapyLoop(success_interval=10).loop_crawl(SitemapPagesSpider)
# SitemapPagesSpider.sitemap_urls.append('https://www.fiserv.com/robots.txt')
# time.sleep(10)
# SitemapPagesSpider.sitemap_urls.append('https://www.teksystems.com/robots.txt')
# SitemapPagesSpider.sitemap_urls =[]
# ScrapyLoop().loop_crawl(SitemapPagesSpider)


#
# print(get_count())
# print(get_count())
# print(get_count())
# def stop_reactor(twist):
#     reactor.stop()


# def add_crawlers():
#     configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
#     crawl_settings = get_project_settings()
#     run = CrawlerRunner(settings=crawl_settings)
#     SitemapPagesSpider.sitemap_urls = ['http://www.teksystems.com/robots.txt']
#     run.crawl(SitemapPagesSpider)
#     deferred = run.join()
#     deferred.addBoth(lambda _: reactor.stop())

def add_crawlers():
    sm_urls.clear()
    sm_urls.append('http://www.teksystems.com/robots.txt')
    SitemapPagesSpider.sitemap_urls = sm_urls.copy()
    runner.crawl(SitemapPagesSpider)
    runner.crawl(UrlExtractor, root='http://www.experis.com', depth=0, allow=keywords.CAREER_PAGE,
                 deny=keywords.CAREER_PAGE_DENY)
    # deferred = runner.join()
    # deferred.addBoth(lambda _: reactor.stop())


sm_urls = ['https://www.fiserv.com/robots.txt']
configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
SitemapPagesSpider.sitemap_urls = sm_urls
crawl_settings = get_project_settings()
runner = CrawlerRunner(settings=crawl_settings)
runner.crawl(SitemapPagesSpider)
# d = runner.join()
# d.addBoth(lambda twist: reactor.stop())
# d.addCallback()
# d.addBoth(stop_reactor)
# d.addBoth(lambda _: reactor.stop())
# d = threads.deferToThread(add_crawlers)
d = threads.deferToThread(run_spider_thread.add_crawlers, param= 'dude')
# d.addBoth(print)
# print('add_crawlers will run.. wait main wait')
reactor.run()  # the script will block here until all crawling jobs are finished

print('reactor is shut down')
