import requests
import xmltodict
import metadata

# poc file
class DecodeSiteMapUrl:
    sm_list = ["/sitemap.xml",
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
            resp = requests.get(url, timeout=5)
            metadata.Metadata().req_resp_size_logger(resp)
            if 200 != resp.status_code:
                #print("site map url not valid:" + url)
                return False
            site_map_dict = xmltodict.parse(resp.text)
            if not bool(site_map_dict):
                #print("site map url not valid:" + url)
                return False
        except:
            #print("site map url not valid:" + url)
            return False
        return True

    def get_from_robots_txt(self, domain_url):
        smurl = ''
        try:
            resp = requests.get(domain_url, timeout=5)
            metadata.Metadata().req_resp_size_logger(resp)
            html_texts = resp.text.lower().split("\n")
            for text in html_texts:
                if "sitemap:" in text:
                    smurl_str = text.split("sitemap:")[1].strip()
                    if smurl_str.startswith("http"):
                        smurl = smurl_str
                        break
        except:
            return smurl
        return smurl


    def get_sitemap_url(self, domain_url) -> object:
        sm_url = self.get_from_robots_txt(domain_url+"/robots.txt")
        if '' != sm_url: return sm_url
        for l in self.sm_list:
            final_url = domain_url+l
            flag = self.is_valid_sm(final_url)
            if flag : return final_url
        return final_url if flag else ''


if __name__ == '__main__':
    met_obj = metadata.Metadata()
    sm_obj = DecodeSiteMapUrl()
    url = met_obj.get_resp_url("ambilight.philips").url
    print(url)
    print(sm_obj.get_sitemap_url(url))
    # print(DecodeSiteMapUrl.get_sitemap_url("www.google.com"))

