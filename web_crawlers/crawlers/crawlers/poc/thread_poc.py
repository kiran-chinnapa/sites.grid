# kill thread running more than x hours
import threading
import time
from threading import Thread
import random


class Test_Thread(Thread):

    def __init__(self):
        self.running = True
        Thread.__init__(self)

    def terminate(self):
        self.running = False

    def run(self):
        print('thread : {} running'.format(threading.get_ident()))
        while self.running:
            time.sleep(random.randrange(1, 5))
        print('thread : {} completed'.format(threading.get_ident()))

threads = []
thread_pool_size = 5
i = 0
# max_seconds = 21600
max_seconds = 10
for j in range(100):
    if i % thread_pool_size == 0:
        for t in threads:
            start = time.time()
            while t.is_alive():
                print('thread : {} is alive'.format(t.ident))
                time.sleep(5)
                if (time.time() - start) > max_seconds:
                    print('killing thread : {} : running more than : {} : seconds'.format(t.ident, max_seconds))
                    t.terminate()
                    break
            t.join()
            print('thread : {} is alive : {}'.format(t.ident, t.is_alive()))
        threads = []
        i = 0
        time.sleep(5)
    t = Test_Thread()
    t.start()
    threads.append(t)
    i = i + 1
for t in threads:
    t.join()
