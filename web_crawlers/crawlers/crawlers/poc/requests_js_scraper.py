# https://docs.python-requests.org/projects/requests-html/en/latest/
from requests_html import HTMLSession

session = HTMLSession()

# url = 'https://careers.teksystems.com/us/en'
url = 'https://careers-obxtek.icims.com/jobs/intro?hashed=-435683781'

response = session.get(url)

response.html.render(sleep=5, keep_page=True, scrolldown=1)

# print(response.html.find('.information"'))

with open('../html/test_requests-html.html', 'w') as f:
    f.write(response.text)

# for ele in response.html.find('button'):
#     if ele.text == 'Search':
#         print(ele)
# for link in response.links:
#     print(link)