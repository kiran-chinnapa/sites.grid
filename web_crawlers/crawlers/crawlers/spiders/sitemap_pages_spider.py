import logging
from scrapy.spiders import SitemapSpider
from scrapy.spiders.sitemap import iterloc
from crawlers.utils import keywords, grid_utils
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
import json
from scrapy import Request


class SitemapPagesSpider(SitemapSpider):
    name = 'sitemap_pages'
    sitemap_urls = []
    sitemap_rules = [
        (keywords.CAREER_PAGE, 'parse_careers'),
    ]

    def __init__(self, *a, **kw):
        self.json_dict = json.loads(grid_utils.query_template)
        self.uncrawl_dict = {"insert": {"rows": []}}
        super(SitemapPagesSpider, self).__init__(*a, **kw)
        logging.info("sitemap_pages spider initialized")

    def start_requests(self):
        meta_info = {'info': self.companies}
        try:
            yield Request('{}{}'.format(self.companies['company_url'],'/robots.txt'), self._parse_sitemap,
                                  meta=meta_info, errback=self.errback)
        except Exception as e:
            self.add_to_uncrawled(meta_info, str(e))

    def errback(self, failure):
        self.add_to_uncrawled(failure.request.meta, '{}:{}'.format(str(failure.type)[1:-1], failure.value))

    def add_to_uncrawled(self, meta_info, error_reason):
        # if not grid_utils.is_row_present('company_url', meta_info['info']['company_url'], self.uncrawl_grid):
        meta_info['info']['Error Reason'] = error_reason
        meta_info['info']['Spider Name'] = SitemapPagesSpider.name
        self.uncrawl_dict["insert"]["rows"].append(meta_info['info'])
        grid_utils.add_row(self.uncrawl_grid, grid_utils.qa_auth_id, self.uncrawl_dict)
        self.uncrawl_dict["insert"]["rows"] = []

    def _parse_sitemap(self, response):
        if response.url.endswith('/robots.txt'):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield Request(url, callback=self._parse_sitemap, meta=response.meta)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                self.logger.warning("Ignoring invalid sitemap: %(response)s",
                                    {'response': response}, extra={'spider': self})
                return

            s = Sitemap(body)
            it = self.sitemap_filter(s)

            if s.type == 'sitemapindex':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self._parse_sitemap, meta=response.meta)
            elif s.type == 'urlset':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    for r, c in self._cbs:
                        if r.search(loc):
                            yield Request(loc, callback=c, meta=response.meta)
                            break

    def parse_careers(self, response):
        yield {
            'Career Page': response.url,
            'Company Website': response.meta['info']['company_url'],
            'Company Name': response.meta['info']['company_name'],
            'DMV Grid Name': response.meta['info']['dmv_grid_name'],
            'DMV Grid Id': response.meta['info']['dmv_grid_id'],
        }