import requests
from bs4 import BeautifulSoup
html = requests.get('https://en.wikipedia.org/wiki/Main_Page')
# print(html.text)
bs = BeautifulSoup(html.text, "html.parser")
titles = bs.find_all(['h1', 'h2','h3','h4','h5','h6'])
print('List all the header tags :', *titles, sep='\n\n')