import csv
import datetime
import logging
import os
import re
import time
import traceback
import bs4
import requests
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import keywords
import metadata
import regex_all
from panda_load_pages import PandasLoadPages
from selenium.webdriver.chrome.options import Options
import threading
threadLock = threading.Lock()

site_id = 0
def get_site_id():
    global site_id
    with threadLock:
        site_id = site_id + 1
    return "{:09d}".format(site_id)

class Mapper:

    def __init__(self, metadata_obj):
        self.meta_data_obj = metadata_obj
        self.site_all_links = set()
        self.pages = []
        self.p = PandasLoadPages()

    # def get_pages_from_keyword(self, keywords):
    #     ret_list = []
    #     for keyword in keywords:
    #         for page in self.pages:
    #             if keyword in page: ret_list.append(page)
    #     return ','.join(ret_list)

    def get_products_selenium(self, url, offering):
        ret_list = set()
        if "products" != offering: return ret_list
        chromeOptions = Options()
        chromeOptions.add_argument('--headless')
        chromeOptions.add_argument('--no-sandbox')
        chromeOptions.add_argument('--disable-dev-shm-usage')
        chromeOptions.add_argument('window-size=1920x1480')
        chromeOptions.headless = True
        # browser = webdriver.Chrome(executable_path=os.getenv("chrome_driver_path"),
        #                            options=chromeOptions)
        browser = webdriver.Chrome(ChromeDriverManager().install(), options=chromeOptions)
        browser.implicitly_wait(10)
        browser.get(url)
        if "salesforce" in url:
            products = WebDriverWait(browser, 10).until(
                expected_conditions.presence_of_element_located(
                    (By.XPATH, "//li[@id='products_menu_item']/button/span[1]")))

            action = ActionChains(browser)
            action.move_to_element(products).perform()

            element = WebDriverWait(browser, 10).until(
                expected_conditions.presence_of_element_located(
                    (By.XPATH, "//div[@id='drawer_products']/div/div")))
            ret_list.add(element.text)
        else:
            element = WebDriverWait(browser, 10).until(
                expected_conditions.presence_of_element_located(
                    (By.XPATH, "//div[@class='ods-unav-hp-accordion__item'] //div[@id='footer-products-section']")))
            ret_list.add(element.text)
        return ', '.join(ret_list)

    def get_products(self, offerings, domain):
        ret_list = set()
        for key in keywords.products:
            if key in offerings:
                for page in self.pages:
                    if key in page:
                        # print (page)
                        prods = page.split(domain)[1].split("/")
                        product = prods[2].strip()
                        if '' != product and "product" not in product :ret_list.add(product)
        return ', '.join(ret_list)

    def site_links_helper(self, browser):
        self.site_all_links.clear()
        anchor_list = WebDriverWait(browser, 15).until(
            expected_conditions.presence_of_all_elements_located((By.TAG_NAME, "a")))
        for anchor in anchor_list:
            try:
                link = anchor.get_attribute("href")
                if link is not None and link.startswith('http'):
                    self.site_all_links.add(link)
            except:
                return None
        return ''

    def populate_site_links(self, url, domain):
        try:
            chromeOptions = Options()
            chromeOptions.headless = True
            # browser = webdriver.Chrome(executable_path=os.getenv("chrome_driver_path"),
            #                            options=chromeOptions)
            browser = webdriver.Chrome(ChromeDriverManager().install(), options=chromeOptions)
            browser.get(url)
            # anchor_list = browser.find_elements_by_tag_name("a")
            ret_val = self.site_links_helper(browser)
            while ret_val is None:
                logging.error("element not available retrying after sleep")
                time.sleep(1)
                ret_val = self.site_links_helper(browser)

            browser.quit()
            # print(self.footer_links)
        except:
            # traceback.print_exc()
            logging.error("unable to page links: {}".format(domain))


    def map(self, rowDict, file_name, domain, resp_url):
        try:
            if '' != resp_url.url:
                logging.info("mapping domain: " + resp_url.url)
                rowDict["Source File"] = file_name
                rowDict["Domain"] = domain
                self.populate_site_links(resp_url.url, domain)
                self.pages, sitemap_urls = self.p.get_domain_pages(resp_url.url, keywords.products + keywords.social_links)
                rowDict["IP Address"] = self.meta_data_obj.get_ip_address(domain)
                rowDict["Site Status"] = "Active"
                rowDict["Site Map Url"] = ', '.join(sitemap_urls)
                rowDict["Last Modified Date"] = self.meta_data_obj.get_last_modified(resp_url.resp, resp_url.url, domain)
                rowDict["Meta Tags"] = self.meta_data_obj.get_meta_tags(resp_url.resp)
                rowDict.update(self.meta_data_obj.get_contact_info_icann(domain))
                rowDict["WhatsApp"] = self.meta_data_obj.get_whatsapp_number(resp_url.resp)
                rowDict['Website County'] = self.meta_data_obj.get_county(
                    self.meta_data_obj.get_zip_code_from_address(rowDict.get("Address")))
                # rowDict.update(self.meta_data_obj.get_address_fields(rowDict["Address"]))
                # fields = ["Website Country", "Website Zip Code", "Website State", "Website City", "Website Street"]
                add_dict = self.meta_data_obj.get_address_fields(rowDict["Address"])
                rowDict["Website Country"] = '' if add_dict.get("Website Country") is None else add_dict.get("Website Country")
                rowDict["Website Zip Code"] = '' if add_dict.get("Website Zip Code") is None else add_dict.get(
                    "Website Zip Code")
                rowDict["Website State"] = '' if add_dict.get("Website State") is None else add_dict.get(
                    "Website State")
                rowDict["Website City"] = '' if add_dict.get("Website City") is None else add_dict.get(
                    "Website City")
                rowDict["Website Street"] = '' if add_dict.get("Website Street") is None else add_dict.get(
                    "Website Street")
                # about_link = self.meta_data_obj.get_about_links(resp_url.resp, domain)
                # if '' == about_link and sitemap_urls:
                    # about_link = self.get_pages_from_keyword(["about","about-us","about_us","about-"+domain])
                # if '' == about_link: about_link = rowDict["Meta Tags"]
                # rowDict["About"] = about_link
                rowDict["About"] = rowDict["Meta Tags"]
                rowDict["Protocol"] = resp_url.url.split(":")[0]
                rowDict["Payments"] = "TRUE" if self.meta_data_obj.is_payments_available(
                    resp_url.resp) else "FALSE"
                rowDict["Log-In"] = "TRUE" if self.meta_data_obj.is_signup_login_available(
                    resp_url.resp) else "FALSE"
                rowDict["Offerings"] = self.meta_data_obj.get_offering(resp_url.resp)
                rowDict["Products"] = self.get_products(rowDict["Offerings"],domain)
                rowDict["Category"] = self.meta_data_obj.get_category(domain)
                rowDict["Age Restriction"] = self.get_age_restriction()# done
                rowDict["Nature of Content"] = self.meta_data_obj.get_nature_of_content(resp_url.resp)
                rowDict["Site ID"] = get_site_id()
                rowDict["Captcha"] = "TRUE" if self.meta_data_obj.is_captcha_enabled(
                    resp_url.resp) else "FALSE"
                rowDict["IP Country"] = self.meta_data_obj.get_ip_country(domain)
                rowDict["ADs"] = self.meta_data_obj.do_ads_exits(resp_url.resp)
                rowDict["Mobile Friendly"] = self.meta_data_obj.is_mobile_friendly(domain)
                rowDict["Website Content Language"] = self.meta_data_obj.get_language(resp_url.resp)
                rowDict["Website Content Supported Languages"] = self.meta_data_obj.get_languages(resp_url.resp)
                rowDict["Website Code Language"] = self.meta_data_obj.get_code_languages(resp_url.resp)
                rowDict["Website Code Language Version"] = self.meta_data_obj.get_web_code_lang_version(resp_url) # done
                rowDict["Business Hours"] = self.meta_data_obj.get_business_hours(domain)
                rowDict["Delivery Service"] = "TRUE" if "Shopping" == rowDict["Category"] else "FALSE"
                rowDict.update(self.get_primary_contact_details()) # done
                rowDict["Primary Social Links"] = self.get_social_links()
                rowDict["Textable Primary Phone"] = rowDict["Primary Phone"]
                rowDict["Google Rating"] = self.meta_data_obj.get_google_rating(domain)
                rowDict["Data Last Updated"] = datetime.datetime.now().strftime("%H:%M:%S %d/%m/%Y")
                rowDict["Hiring"] = self.meta_data_obj.is_hiring(resp_url.resp)

            else:
                rowDict["Site Status"] = "Inactive"
        except:
            traceback.print_exc()

    def get_social_links(self):
        social_links = []
        for link in self.pages:
            for sk in keywords.social_links:
                if sk in link:
                    social_links.append(link)

        return ', '.join(social_links)

    # def get_primary_address_from_pages(self):
    #     p_address = ''
    #     for link in self.pages:
    #         if "contact" in link.lower():
    #             out_link = link
    #             # print(out_link)
    #             p_address = self.meta_data_obj.get_address_from_page(out_link)
    #             if '' != p_address.strip(): return p_address
    #
    #     for link in self.pages:
    #         if "about" in link.lower():
    #             out_link = link
    #             p_address = self.meta_data_obj.get_address_from_page(out_link)
    #             if '' != p_address: return p_address
    #     return p_address



    def get_age_restriction(self):
        age_key_word =['terms', 'privacy', 'policy', 'agreement']
        age = ''
        try:
            for key in age_key_word:
                for page in self.site_all_links:
                    if key in page:
                        print(page)
                        soup = bs4.BeautifulSoup(requests.get(page, timeout=5).text, 'lxml')
                        match = re.search(regex_all.age_restriction, soup.text)
                        if match is not None:
                            age = match.group()

                if '' != age.strip(): return self.get_age_from_string(age)
        except:
            # traceback.print_exc()
            logging.error("unable to age restriction for domain")
        return age


    def get_age_from_string(self, str):
        _len = len(str)
        for i in range(0,_len,1):
            if i+3 <= _len and not str[i].isdigit() and str[i+1].isdigit() and str[i+2].isdigit() and not str[i+3].isdigit():
                age = str[i+1:i+3]
                if int(age) <22:
                    return age+"+"
        return ''


    def get_primary_contact_details(self):
        key_word = ['contact']
        primary_dict = {'Primary Address': '',
                    'Primary Address Geolocation':'',
                    'Primary Address County':'',
                    'Primary Address City':'',
                   'Primary Address Country':'',
                    'Primary Address Zip Code':'',
                        'Primary E-Mail':'',
                        'Primary Phone':''}
        try:
            for key in key_word:
                for page in self.site_all_links:
                    if key in page:
                        print(page)
                        soup = bs4.BeautifulSoup(requests.get(page, timeout=5).text, 'lxml')
                        for line in soup.text.split("\n"):
                            match = re.search(regex_all.contact_info, line)
                            if match is not None:
                                if self.is_contact_dict_mapped(line, primary_dict): return primary_dict

        except:
            # traceback.print_exc()
            logging.error("unable to contact details for domain")

        return primary_dict

    def is_contact_dict_mapped(self, line, dict):

        if '' == dict['Primary E-Mail']: dict['Primary E-Mail'] = self.meta_data_obj.get_primary_email(line)
        if '' == dict['Primary Phone']: dict['Primary Phone'] = self.meta_data_obj.get_primary_phone(line)
        # if '' == dict['Primary Address']: self.meta_data_obj.get_primary_address_dict_old(line, dict)
        if '' == dict['Primary Address']: self.meta_data_obj.get_primary_address_dict(line, dict)

        return True if '' != dict['Primary Address'] and '' != dict['Primary E-Mail'] and '' != dict['Primary Phone'] else False




def main():
    m = metadata.Metadata()
    mapper = Mapper(m)
    # with open("source_sites.csv") as read_obj:
    #     csv_reader = csv.reader(read_obj)
    #     for csv_row in csv_reader:
    domain = "cloud.sap.com"
    print(domain)
    resp_url = m.get_resp_url(domain)
    # mapper.populate_site_links(resp_url.url, domain)
    mapper.pages, mapper.sm_urls = mapper.p.get_domain_pages(resp_url.url,[])
    print('sitemap url:{}'.format(mapper.sm_urls))
    # offering = m.get_offering(resp_url.resp)
    # print(offering)
    # print(mapper.get_products(offering, domain ))
    # print(m.get_contact_info_icann(domain))
    # exit(0)




if __name__ == "__main__":
    main()

