import re
import time
import traceback
import bs4
import requests
import scrapy
import logging
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy import signals
from scrapy.utils.log import configure_logging
# Multi-threaded implementation of scrappy.
from twisted.internet import reactor



href_regex = "href=[\"\'](.*?)[\"\']"

def load_sitemap_urls(sm_url):
    sitemaps = []
    try:
        # print("parse_sitemap {}".format(sm_url))
        html_text = requests.get(sm_url, timeout=5).text
        soup = bs4.BeautifulSoup(html_text, 'lxml')
        locs = soup.findAll("loc")
        for loc in locs:
            loc_str = loc.string
            if "sitemap" in loc_str:
                sitemaps.append(loc_str)
    except:
        traceback.print_exc()
    return sitemaps

class WebPagesSpider(scrapy.Spider):
    name = "web-pages"

    def start_requests(self):
        # print(self.sitemap_url)
        urls = load_sitemap_urls(self.sitemap_url)
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    # @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     spider = super(WebPagesSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    #     return spider
    #
    # def spider_closed(self, spider):
    #     spider.logger.warning('Spider closed: %s', spider.name)

    def parse(self, response):
        if -1 != response.text.find("href"):
            hrefs = re.findall(href_regex, response.text)
            self._pages.extend(hrefs)
        else:
            soup = bs4.BeautifulSoup(response.text,'lxml')
            locs = soup.findAll("loc")
            for loc in locs:
                self._pages.append(loc.string)


if __name__ == "__main__":
    # sm_url = "https://www.sony.com/electronics/gwtsitemapindex.xml"
    sm_url ="https://www.merriam-webster.com/sitemap-ssl/sitemap_index.xml"

    pages = []
    # sm_url = "https://www.google.com/sitemap.xml"
    start = time.time()
    # process = CrawlerProcess({'LOG_LEVEL': 'INFO'})
    # process.crawl(WebPagesSpider,sitemap_url=sm_url,_pages=pages)
    # process.start()

    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        level=logging.ERROR
    )

    # configure_logging({'LOG_LEVEL': 'INFO'})
    runner = CrawlerRunner()
    d = runner.crawl(WebPagesSpider,sitemap_url=sm_url,_pages=pages)
    d.addBoth(lambda _: reactor.stop())
    reactor.run(installSignalHandlers=0)

    print("total pages loaded:{}".format(len(pages)))
    print(pages[len(pages) -1])
    print("time taken in seconds: {}".format(time.time() - start))