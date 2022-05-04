from unittest import TestCase
from crawlers.utils import crawl_utils


class TestCrawlUtils(TestCase):

    def test_get_job_links(self):
        company_website = 'http://www.teksystems.com'
        result = crawl_utils.get_job_links(company_website)
        print(result)
        self.assertTrue(len(result) > 0)
