import traceback
from seleniumbase import BaseCase
from requests_html import HTMLSession


# https://www.youtube.com/watch?v=DlZH8f0dc4E
# add --headless --browser=chrome to run configuration for headless mode
class SearchButton(BaseCase):

    def scrape_js(self, url):
        print('scraping url:{}'.format(url))
        session = HTMLSession()
        # url = 'https://careers.teksystems.com/us/en/search-results?keywords=Java%20Developer&p=ChIJCzYy5IS16lQRQrfeQ5K5Oxw&location=USA'
        response = session.get(url)
        response.html.render(sleep=5, keep_page=True, scrolldown=1)
        for job_item in response.html.find('div.information > span > a'):
            print(job_item.links)


    def test_search_button_teksystems(self):
        try:
            # open page
            self.open("https://careers.teksystems.com/us/en")
            # fill in the text box
            self.sleep(2)
            self.click('#keywordSearch')
            self.send_keys('#keywordSearch', 'Java Developer')
            self.sleep(1)
            self.click('#gllocationInput')
            self.send_keys('#gllocationInput', 'USA')
            # click the search button
            self.sleep(5)
            self.click("#ph-search-backdrop")
            # verify search results
            self.sleep(5)
            print('**********************')
            url = self.driver.current_url
            self.scrape_js(url)
            # print(self.get_element(".job-category"))
            # if self.assert_element_present(".information"):
            #     element = self.get_element(".information")
            #     text = element.text
            #     print('found dude:{}'.format(text))
            # else:
            #     print('not found dude')
        except:
            traceback.print_exc()
        # self.sleep(5)
        # html = self.driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
        # html = self.driver.page_source
        # with open('../html/test_selenium.html', 'w') as f:
        #     f.write(html)
        # self.sleep(5)
