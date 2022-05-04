import time
from twisted.internet import threads, reactor


class POCSpider():
    spider_list = []


class POCSpider2():
    def __init__(self, root):
        self.source = root

    def get_source(self):
        return self.source


lst = ['bro', 'yup']


# POCSpider.spider_list = lst.copy()
poc2 = None
for l in lst:
    poc2 = POCSpider2(root=l)
    print('before clear')
# print(POCSpider.spider_list)
print(poc2.get_source())
lst.clear()
print('after clear')
print(poc2.get_source())


