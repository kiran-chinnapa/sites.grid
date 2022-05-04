import sys
import threading
import socket
import time
import requests


class  MyThread(threading.Thread):
    def __init__(self, thread_name, url, _file):
        threading.Thread.__init__(self)
        self.url = url
        self._file = _file

    def run(self):
        # with open('domain_check', 'a') as out_file:
        try:
            hostname = socket.gethostbyname(self.url)
            # sitemap_url = 'http://' + self.url + '/sitemap.xml'
            # resp = requests.head(sitemap_url, timeout=5)
            #
            # if 200 != resp.status_code:
            #     sitemap_url = 'http://' + self.url + '/sitemap_index.xml'
            #     resp = requests.head(sitemap_url, timeout=5)
            #     if 200 != resp.status_code:
            #         self._file.write(self.url+":none\n")
            #         return
            #
            # self._file.write(self.url+":"+hostname+":"+sitemap_url+"\n")
            self._file.write(self.url + ":" + hostname + "\n")
        except:
            self._file.write(self.url+":none\n")


start = time.time()
with open('domain_check', 'a') as out_file:
    with open(sys.argv[2]) as in_file:
        i = 1
        threads = []
        while 1:
            if i == 35 + int(sys.argv[1]):
                break
            line = in_file.readline()
            if i > 35:
                url = "www."+line.split(' ')[0]+".com"
                t = MyThread(thread_name="thread-" + str(i), url=url, _file=out_file)
                t.start()
                threads.append(t)
            i = i + 1
        for t in threads:
            t.join()

print('time elapsed:'+ str(time.time() - start))
