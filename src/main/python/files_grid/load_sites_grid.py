import sys
import threading
import socket
import time

class  MyThread(threading.Thread):
    def __init__(self, thread_name, url, _file):
        threading.Thread.__init__(self)
        self.url = url
        self._file = _file

    def run(self):
        try:
            hostname = socket.gethostbyname(self.url)
            self._file.write(self.url + "," + hostname + ",Yes\n")
        except:
            self._file.write(self.url+",,No\n")


start = time.time()

class Controller:
    def spin_threads(thread_size, in_file, out_file):
        i = 0
        threads = []
        for line in in_file:
            i = i + 1
            if i == thread_size:
                for th in threads:
                    th.join()
                threads = []
                i = 0
            url = "www." + line.split(' ')[0] + ".com"
            t = MyThread(thread_name="thread-" + str(i), url=url, _file=out_file)
            t.start()
            threads.append(t)
        for th in threads:
            th.join()


def main():
    with open('sites.grid.csv', 'a') as out_file:
        out_file.write("Domain,IP Address,Active\n")
        with open(sys.argv[1]) as in_file:
            for l in range(35):
                in_file.readline()
            Controller.spin_threads(int(sys.argv[2]), in_file, out_file)

    print('time elapsed:'+ str(time.time() - start))

if __name__ == "__main__":
    main()
