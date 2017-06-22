from bs4 import BeautifulSoup
from pymongo import MongoClient
from Queue import Queue
import threading
import urllib
import urllib2




headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}



#
# for pageNo in range(2000, 2295):
#     url = baseURL + str(pageNo) + "#comments"
#     req = urllib2.Request(url, headers=headers)
#     response2 = urllib2.urlopen(req)
#     content = response2.read()
#     soup = BeautifulSoup(content, 'lxml')
#
#     divs = soup.find_all("div", {"class" : "row"})
#     print pageNo
#     for div in divs:
#         text = div.p.get_text()
#         likeAndDis = div.find_all("span")
#         up = likeAndDis[1].get_text()
#         down = likeAndDis[2].get_text()
#         roast = {"content" : text, "up" : up, "down" : down}
#         roasts = db.roasts
#         roast_id = roasts.insert_one(roast).inserted_id


class thread_crawl(threading.Thread):
    def __init__(self, threadID, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.q = q
        self.baseURL = "http://jandan.net/duan/page-"

    def run(self):
        print "Starting " + self.threadID
        self.jandan_spider()
        print "Exiting ", self.threadID

    def jandan_spider(self):
        # page = 1
        while True:
            if self.q.empty():
                break
            else:
                page = self.q.get()
                print "crawling %s" %page
                url = self.baseURL + str(page) + "#comments"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}

                timeout = 4
                while timeout > 0:
                    timeout -= 1
                    try:
                        req = urllib2.Request(url, headers=headers)
                        response2 = urllib2.urlopen(req)
                        content = response2.read()
                        data_queue.put(content)
                        break
                    except Exception, e:
                        print 'qiushi_spider', e
                if timeout < 0:
                    print 'timeout', url


class thread_parser(threading.Thread):
    def __init__(self, threadID, queue, lock):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.queue = queue
        self.lock = lock
        MONGO_HOST = "ds161001.mlab.com"
        MONGO_PORT = 61001
        MONGO_DB = "blog"
        MONGO_USER = "hanyu2"
        MONGO_PASS = "yuhan0YH*"
        connection = MongoClient(MONGO_HOST, MONGO_PORT)
        db = connection[MONGO_DB]
        db.authenticate(MONGO_USER, MONGO_PASS)
        self.collection = db["roasts"]

    def run(self):
        print 'starting ', self.threadID
        global total, exitFlag_Parser
        while not exitFlag_Parser:
            try:
                item = self.queue.get(False)
                if not item:
                    pass
                self.parse_data(item)
                self.queue.task_done()
            except:
                pass
        print 'Exiting ', self.threadID

    def parse_data(self, item):
        global total
        try:
            soup = BeautifulSoup(item, 'lxml')
            divs = soup.find_all("div", {"class": "row"})
            for div in divs:
                try:
                    text = div.p.get_text()
                    likeAndDis = div.find_all("span")
                    up = likeAndDis[1].get_text()
                    down = likeAndDis[2].get_text()
                    roast = {"content": text, "up": int(up), "down": int(down)}
                    roasts = self.collection
                    with self.lock:
                        roast_id = roasts.insert_one(roast).inserted_id
                except Exception, e:
                    print 'site in result', e
        except Exception, e:
            print 'parse_data', e
        with self.lock:
            total += 1

data_queue = Queue()
exitFlag_Parser = False
lock = threading.Lock()
total = 0

def main():
    pageQueue = Queue(2297)
    for page in range(1, 2297):
        pageQueue.put(page)

    crawlthreads = []
    crawlList = ["crawl-1", "crawl-2", "crawl-3","crawl-4", "crawl-5", "crawl-6","crawl-7", "crawl-8", "crawl-9"]

    for threadID in crawlList:
        thread = thread_crawl(threadID, pageQueue)
        thread.start()
        crawlthreads.append(thread)

    parserthreads = []
    parserList = ["parser-1", "parser-2", "parser-3","parser-4", "parser-5", "parser-6","parser-7", "parser-8", "parser-9"]
    for threadID in parserList:
        thread = thread_parser(threadID, data_queue, lock)
        thread.start()
        parserthreads.append(thread)

    while not pageQueue.empty():
        pass

    for t in crawlthreads:
        t.join()

    while not data_queue.empty():
        pass
    global exitFlag_Parser
    exitFlag_Parser = True

    for t in parserthreads:
        t.join()
    print "Exiting Main Thread"

if __name__ == '__main__':
    main()