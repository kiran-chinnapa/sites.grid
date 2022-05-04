from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor

from crawlers.spiders.sitemap_pages_spider import SitemapPagesSpider
from crawlers.spiders.url_extraction_spider import UrlExtractor
from crawlers.utils import keywords


def add_crawlers(param):
    print('param value {}'.format(param))
    crawl_settings = get_project_settings()
    runner = CrawlerRunner(settings=crawl_settings)
    sm_urls = ['http://www.teksystems.com/robots.txt']
    SitemapPagesSpider.sitemap_urls = sm_urls.copy()
    runner.crawl(SitemapPagesSpider)
    runner.crawl(UrlExtractor, root='http://www.experis.com', depth=0, allow=keywords.CAREER_PAGE,
                 deny=keywords.CAREER_PAGE_DENY)
    deferred = runner.join()
    deferred.addBoth(lambda _: reactor.stop())