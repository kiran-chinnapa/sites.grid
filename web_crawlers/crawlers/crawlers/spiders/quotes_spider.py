import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class QuotesSpider(scrapy.Spider):
    name = 'quotes'

    def start_requests(self):
        # urls = ['http://quotes.toscrape.com/']
        urls = ['https://www.linkedin.com/company/1901-group', 'https://twitter.com/1901group', 'https://www.facebook.com/1901group', 'http://www.1901group.com/', 'http://www.1901group.com/about/', 'http://www.1901group.com/about-leadership/', 'http://www.1901group.com/about-sme/', 'http://www.1901group.com/about-whoweserve/', 'http://www.1901group.com/about-partners/', 'http://www.1901group.com/about-certs/', 'http://www.1901group.com/about-awards/', 'http://www.1901group.com/whatwedo/', 'http://www.1901group.com/whatwedo-cloud/', 'http://www.1901group.com/whatwedo-cyber/', 'http://www.1901group.com/whatwedo-managedsvc/', 'http://www.1901group.com/whatwedo-engineeringsvc/', 'http://www.1901group.com/whatwedo-appdev/', 'http://www.1901group.com/whatwedo-in3/', 'http://www.1901group.com/whatwedo-approach/', 'http://www.1901group.com/whatwedo-infrastructure/', 'http://www.1901group.com/about-whoweserve/#contract', 'http://www.1901group.com/careers/', 'http://www.1901group.com/careers-model/', 'http://www.1901group.com/careers-benefits/', 'http://www.1901group.com/careers-community/', 'http://www.1901group.com/careers-jobs/', 'http://www.1901group.com/resources', 'http://www.1901group.com/resources-pr/', 'http://www.1901group.com/resources-news/', 'http://www.1901group.com/resources-blog/', 'http://www.1901group.com/resources-media/', 'http://www.1901group.com/resources-events/', 'http://www.1901group.com/resources-podcast/', 'http://www.1901group.com/careers-community', 'http://www.1901group.com/careers-model', 'http://www.1901group.com/careers-benefits', 'https://www.youtube.com/embed/chKFL0UN9lw', 'https://twitter.com/1901Group?ref_src=twsrc%5Etfw', 'http://www.1901group.com/careers', 'http://www.1901group.com/contact', 'http://www.1901group.com/policies-and-notices/#privacy', 'http://www.1901group.com/policies-and-notices/#DMCA', 'http://www.1901group.com/policies-and-notices/#AdChoices', 'http://www.1901group.com/policies-and-notices/#DoNotSell', 'http://www.1901group.com/policies-and-notices/#508']
        print('total url count:{}'.format(len(urls)))
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_url)

    def parse_url(self, response):
        title = response.css('title::text').extract()
        # yield {'titletext': title}
        # following links in page
        # response.follow(next_page,callback=self.parse)
        # how to use pagination
        print('title::{}'.format(title))
        # yield scrapy.Request("http://www.teksystems.com", callback=self.parse_url)

    # def parse_url(self, response):
    #     print('in parse url::{}'.format(response.url))
    #     yield True


if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(QuotesSpider)
    process.start()
