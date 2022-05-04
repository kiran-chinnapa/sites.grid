import time
import traceback
import bs4
import requests
import re

# Single threaded implementation
class ScrapePages:
    href_regex = "href=[\"\'](.*?)[\"\']"

    def map_links(self, html_text, pages):
        try:
            print("map_links {}".format(len(pages)))
            hrefs = re.findall(ScrapePages.href_regex, html_text)
            if len(hrefs) > 0:
                for href in hrefs:
                    pages.append(href)
                    # print(href)
        except:
            traceback.print_exc()

    def parse_sitemap(self, url, pages):
        try:
            print("parse_sitemap {}".format(url))
            html_text = requests.get(url, timeout=5).text
            if -1 != html_text.find("href"):
                self.map_links(html_text, pages)
            elif -1 != html_text.find("loc"):
                soup = bs4.BeautifulSoup(html_text, 'lxml')
                locs = soup.findAll("loc")
                for loc in locs:
                    loc_str = loc.string
                    if "sitemap" in loc_str:
                        self.parse_sitemap(loc_str, pages)
        except:
            traceback.print_exc()
        return pages


if __name__ == '__main__':
    sm_url = "https://www.sony.com/electronics/gwtsitemapindex.xml"
    # sm_url ="https://www.google.com/sitemap.xml"
    start = time.time()
    scrapePages = ScrapePages()
    pages = []
    try:
        scrapePages.parse_sitemap(sm_url, pages)
        print("pages_len:{}".format(len(pages)))
    except:
        traceback.print_exc()
        print("sitemap not available")
    print(time.time() - start)
