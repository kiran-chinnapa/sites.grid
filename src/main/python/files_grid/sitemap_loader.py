import logging
import re
import time

import bs4
import requests
import xmltodict

import metadata
import regex_all

# poc file
class SitemapLoader:

    def __init__(self):
        self.sm_list = ["/sitemap.xml",
               "/feeds/posts/default?orderby=updated",
               "/sitemap.xml.gz",
               "/sitemap_index.xml",
               "/s2/sitemaps/profiles-sitemap.xml",
               "/sitemap.php",
               "/sitemap_index.xml.gz",
               "/vb/sitemap_index.xml.gz",
               "/sitemapindex.xml",
               "/sitemap.gz",
               "/sitemap_news.xml",
               "/sitemap-index.xml",
               "/sitemapindex.xml",
               "/sitemap-news.xml",
               "/post-sitemap.xml",
               "/page-sitemap.xml",
               "/portfolio-sitemap.xml",
               "/home_slider-sitemap.xml",
               "/category-sitemap.xml",
               "/author-sitemap.xml"]

    def is_valid_sm(self, url):
        try:
            resp = requests.get(url, timeout= 5)
            if 200 != resp.status_code:
                # print("site map url not valid:" + url)
                return False
            site_map_dict = xmltodict.parse(resp.text)
            if not bool(site_map_dict):
                # print("site map url not valid:" + url)
                return False
        except:
            # print("site map url not valid:" + url)
            return False
        return True

    def get_sub_urls(self, sm_url):
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
            logging.error("unable to load sitmap for: {}".format(sm_url))
        return sitemaps

    def get_from_robots_txt(self, robots_sm_url):
        smurls = []
        try:
            html_texts = requests.get(robots_sm_url, timeout=5).text.split("\n")
            for text in html_texts:
                lower_text = text.lower()
                lit_idx = lower_text.find("sitemap:")
                if -1 != lit_idx:
                    literal = text[lit_idx:lit_idx+8]
                    smurl_str = text.split(literal)[1].strip()
                    if smurl_str.startswith("http"):
                        smurls.append(smurl_str)
        except:
            return smurls
        return smurls


    def get_urls_from_bot(self, domain_url) -> object:
        sm_urls = self.get_from_robots_txt(domain_url+"/robots.txt")
        if sm_urls: return sm_urls
        for l in self.sm_list:
            final_url = domain_url+l
            flag = self.is_valid_sm(final_url)
            if flag :
                sm_urls.append(final_url)
                return sm_urls
        return sm_urls

    def load_pages(self, sm_urls, pages):
        try:
            for smurl in sm_urls:
                response = requests.get(smurl, timeout=5)
                # if -1 != response.text.find("href"):
                #     hrefs = re.findall(regex_all.href_regex, response.text)
                #     pages.extend(hrefs)
                # else:
                soup = bs4.BeautifulSoup(response.text, 'lxml')
                locs = soup.findAll("loc")
                for loc in locs:
                    pages.append(loc.string)
        except:
            return


    def get_domain_pages(self, domain_url):
        pages = []
        sm_urls = self.get_urls_from_bot(domain_url)
        sub_sm_urls =[]
        for sm_url in sm_urls:
            sub_sm_urls.extend(self.get_sub_urls(sm_url))
        if sub_sm_urls:
            #get pages for sub
            self.load_pages(sub_sm_urls, pages)
        else:
            #get pages for sm_urls
            self.load_pages(sm_urls, pages)
        return pages


def main():
    start = time.time()
    sm_obj = SitemapLoader()
    m = metadata.Metadata()
    domains = [
        # 'ebay.com',
        'motorola.com'
               ]
    for dom in domains:
        pages =sm_obj.get_domain_pages(m.get_resp_url(dom).url)
        print("pages len:{}".format(len(pages)))
        print(pages[0])
    print("total time {}".format(time.time() - start))


if __name__ == "__main__":
    main()


