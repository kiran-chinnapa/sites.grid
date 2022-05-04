import logging
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from crawlers.utils import keywords


class AllLinksExtractor(Spider):
    name = 'all-links-extractor'

    def __init__(self, *args, **kwargs):
        self.le = LinkExtractor(
            # allow=keywords.CAREER_PAGE,
            # allow='Careers',
            unique=True, process_value=self.process_value, deny_extensions=None,
            strip=True)
        super(AllLinksExtractor, self).__init__(*args, **kwargs)
        logging.info("AllLinkExtractor spider initialized")

    def process_value(self, value):
        return value.lower()

    def start_requests(self):
        # comp_urls = ['https://www.cogniertechnology.com/']
        # comp_urls = ['https://careers.microsoft.com/us/en/search-results']
        # comp_urls = ['https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions']
        # comp_urls = ['https://www.accenture.com/us-en/careers/jobsearch?jk=&sb=1&pg=1&is_rj=0']
        # comp_urls = ['https://www.amazon.jobs/en/job_categories/software-development']
        comp_urls = ['https://technomile.com/careers/']
        for comp_url in comp_urls:
            yield Request(comp_url)

    def parse(self, response):
        all_urls = self.get_all_links(response)
        for url in all_urls:
            yield {
                'sub-link': url
            }
            print(url)

    def get_all_links(self, response):
        links = self.le.extract_links(response)
        str_links = []
        for link in links:
            str_links.append(link.url)
            # print(link.text)
        return str_links


if __name__ == '__main__':
    crawl_settings = get_project_settings()
    crawl_settings['LOG_LEVEL'] = 'ERROR'
    # uncomment below line while debugging.
    # crawl_settings['FEEDS'] = {
    #         "sub-links.jl": {"format": "json"},
    #     }
    crawl_settings['ITEM_PIPELINES'] = {}
    crawl_settings['DOWNLOADER_MIDDLEWARES'] = {
        'crawlers.middlewares.JSMiddleware': 543,
    }
    #  comment above two lines while running on server.
    process = CrawlerProcess(settings=crawl_settings)
    process.crawl(AllLinksExtractor)
    process.start()
