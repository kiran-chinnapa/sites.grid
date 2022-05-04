###################
# You can find the article about this gist here:
# https://primates.dev/find-all-urls-of-a-website-in-a-few-seconds-python/
####################
import logging
import re
import time
import traceback
import bs4
import requests
import xmltodict
from bs4 import BeautifulSoup as Soup
import pandas as pd
import hashlib
# Pass the headers you want to retrieve from the xml such as ["loc", "lastmod"]
import grid_utils
import keywords
import metadata
import regex_all


class PandasLoadPages:

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
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36'}

    def is_valid_sm(self, url):
        try:
            resp = requests.get(url, timeout=5)
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

    def parse_sitemap(self, url, headers, grid_id, auth_id, max_insert_rows=0, resp_url_obj =None):
        panda_out = pd.DataFrame()
        try:
            if "" == url.strip():
                return pd.DataFrame()

            logging.info("pandas processing url {}".format(url))
            resp = requests.get(url, timeout=5)
            metadata.Metadata().req_resp_size_logger(resp)

            if 403 == resp.status_code:
                resp = requests.get(url, timeout=5, headers= self.headers)
                metadata.Metadata().req_resp_size_logger(resp)
            # we didn't get a valid response, bail
            if 200 != resp.status_code:
                return pd.DataFrame()
           # logging.info("pandas processing line 70 ")
            # BeautifulSoup to parse the document
            soup = Soup(resp.content.decode('utf-8', 'ignore'), "xml")
            #logging.info("pandas processing line 73 ")
            # find all the <url> tags in the document
            urls = soup.findAll('url')
            sitemaps = soup.findAll('sitemap')
            new_list = ["Source"] + headers
            panda_out_total = pd.DataFrame([], columns=new_list)
            # logging.info("panda_out_total row size line 79:{}".format(panda_out_total.shape[0]))
            if not urls and not sitemaps:
                return pd.DataFrame()

            # Recursive call to the the function if sitemap contains sitemaps
            if sitemaps:
                for u in sitemaps:
                    test = u.find('loc').string
                    panda_recursive = self.parse_sitemap(test, headers, grid_id, auth_id)
                    panda_out_total = pd.concat([panda_out_total, panda_recursive], ignore_index=True)
                    logging.info("panda_recursive row size :{}".format(panda_recursive.shape[0]))
                    logging.info("panda_out_total row size :{}".format(panda_out_total.shape[0]))
                    if panda_out_total.shape[0] > max_insert_rows and panda_out_total.shape[0] > panda_recursive.shape[0] and grid_id != None:
                        grid_utils.GridUtils.releaseDataFrame(panda_out_total, url, grid_id, auth_id, max_insert_rows,resp_url_obj)
                        panda_out_total = pd.DataFrame()
           # logging.info("pandas processing line 89 ")
            # storage for later...
            out = []

            # Creates a hash of the parent sitemap
            hash_sitemap = hashlib.md5(str(url).encode('utf-8')).hexdigest()

            # Extract the keys we want
            for u in urls:
                values = [hash_sitemap]
                for head in headers:
                    loc = None
                    loc = u.find(head)
                    if not loc:
                        loc = "None"
                    else:
                        loc = loc.string
                    values.append(loc)
                out.append(values)

            # Create a dataframe
            panda_out = pd.DataFrame(out, columns= new_list)
            # logging.info("pandas processing line 111 ")
            # If recursive then merge recursive dataframe
            if not panda_out_total.empty:
                panda_out = pd.concat([panda_out, panda_out_total], ignore_index=True)
        except Exception as e:
            logging.error("pandas processing error: {}".format(e))
            # traceback.print_exc()
            try:
                raise TypeError("Again !?!")
            except:
                pass

        #returns the dataframe
        # logging.info("row count of data-frame:{}".format(panda_out.shape[0]))
        # print("row count of data-frame:{},{}".format(panda_out.shape[0],panda_out.shape[1]))
        return panda_out

    def get_from_robots_txt(self, robots_sm_url):
        smurls = []
        try:
            resp = requests.get(robots_sm_url, timeout=5)
            if resp.status_code == 403:
                resp = requests.get(robots_sm_url, timeout=5, headers=self.headers)
            html_texts = resp.text.split("\n")
            for text in html_texts:
                lower_text = text.lower()
                lit_idx = lower_text.find("sitemap:")
                if -1 != lit_idx:
                    literal = text[lit_idx:lit_idx+8]
                    smurl_str = text.split(literal)[1].strip()
                    if smurl_str.startswith("http") and not smurl_str.endswith(".gz"):
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

    def get_sub_urls(self, sm_url):
        sitemaps = []
        try:
            # print("parse_sitemap {}".format(sm_url))
            html_text = requests.get(sm_url, timeout=5).text
            soup = bs4.BeautifulSoup(html_text, 'lxml')
            locs = soup.findAll("loc")
            for loc in locs:
                loc_str = loc.string.strip()
                if "sitemap" in loc_str and not loc_str.endswith(".gz"):
                    sitemaps.append(loc_str)
        except:
            # traceback.print_exc()
            logging.error("unable to load sitmap for: {}".format(sm_url))
        return sitemaps

    def load_pages(self, sm_urls, pages, filter):
        try:
            if len(filter) == 0: return
            for smurl in sm_urls:
                dataframe = self.parse_sitemap(smurl, ["loc"], None, None)
                if not dataframe.empty:
                    df_list = dataframe["loc"].tolist()
                    del dataframe
                    for df in df_list:
                        if "investors" in df:
                            print("found:" + df)
                        for fltr in filter:
                            if fltr in df and not df.strip().endswith(".gz"):
                                pages.append(df)
                logging.info("loading pages size:{}:{}".format(len(pages), smurl))
                # print("loading pages size:{}:{}".format(len(pages), smurl))
        except:
            logging.error("error getting pages for: {}".format(smurl))

    def get_domain_pages(self, domain_url, filter):
        pages = []
        sm_urls = self.get_urls_from_bot(domain_url)
        sub_sm_urls = []
        for sm_url in sm_urls:
            sub_sm_urls.extend(self.get_sub_urls(sm_url))
        if sub_sm_urls:
            # get pages for sub
            self.load_pages(sub_sm_urls, pages, filter)
        else:
            # get pages for sm_urls
            self.load_pages(sm_urls, pages, filter)
        return pages, sm_urls

    def get_all_page_links_helper(self, url):
        try:
            links =[]
            resp = requests.get(url, timeout=5)
            metadata.Metadata().req_resp_size_logger(resp)
            soup = bs4.BeautifulSoup(resp.text, 'lxml')
            for link in soup.find_all('a'):
                links.append(link.get('href'))
        except Exception as e:
            logging.error("get_all_page_links_helper processing error: {}".format(e))
        return links

    def get_all_page_links(self, url):
        file_links =[]
        try:
            list = self.get_all_page_links_helper(url)
            for link in list:
                if None != link and re.search(regex_all.resume, link, re.IGNORECASE) is not None:
                    sub_links = self.get_all_page_links_helper(link)
                    for s_link in sub_links:
                        if None != s_link and re.search(regex_all.file_extension, s_link, re.IGNORECASE) is not None:
                            if not s_link.startswith('http'):
                                file_links.append('{}/{}'.format(link, s_link))
                            else:
                                file_links.append(s_link)
        except Exception:
            logging.error("get_all_page_links processing error:url {}: {}".format(url,traceback.format_exc()))
        return file_links



if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='panda_load_pages.log')
    # start = time.time()
    # # sl = sitemap_loader.SitemapLoader()
    p = PandasLoadPages()
    # m = metadata.Metadata()
    # sm_url = p.get_urls_from_bot(m.get_resp_url("coreyms").url)
    # print(sm_url[0])
    # # sm_url ="http://coreyms.com/sitemap.xml"
    # try:
    #     dataframe = p.parse_sitemap(sm_url[0], ["loc", "lastmod"],'6084301494a030356bf98c12','b1427e2f-81f4-4280-b56b-305ba8f36b5c')
    #     if not dataframe.empty:
    #         print("pages found")
    #         print (dataframe['loc'].tolist())
    # except:
    #     traceback.print_exc()
    #     print("sitemap not available")
    # # pages, smurl = p.get_domain_pages(m.get_resp_url("coreyms.com").url, [])
    # # print(len(pages))
    # # print(time.time() - start)
    # url = m.get_resp_url("flatex.at").url
    # print(p.get_all_page_links(url))



    print(p.get_all_page_links(" https://www.lgit.co.za"))
