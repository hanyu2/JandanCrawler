from bs4 import BeautifulSoup
import urllib
import urllib2
from pymongo import MongoClient

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}

MONGO_HOST = "ds161001.mlab.com"
MONGO_PORT = 61001
MONGO_DB = "blog"
MONGO_USER = "hanyu2"
MONGO_PASS = "yuhan0YH*"
connection = MongoClient(MONGO_HOST, MONGO_PORT)
db = connection[MONGO_DB]
db.authenticate(MONGO_USER, MONGO_PASS)
collection = db["ooxx"]

for pageNo in range(1, 134):
    url = "http://jandan.net/ooxx/page-"+str(pageNo)+"#comments"
    req = urllib2.Request(url, headers=headers)
    response2 = urllib2.urlopen(req)
    content = response2.read()
    soup = BeautifulSoup(content, 'lxml')
    divs = soup.find_all("div", {"class": "row"})
    print pageNo
    for div in divs:
        try:
            imgurl = div.find("img")["src"]
            if(imgurl.endswith("gif")):
                imgurl = div.find("img")["org_src"]
            fullurl = "http:"+imgurl
            likeAndDis = div.find_all("span")
            up = likeAndDis[1].get_text()
            down = likeAndDis[2].get_text()
            ooxx = {"imgurl": fullurl, "up": int(up), "down": int(down)}
            collection.insert_one(ooxx)
        except Exception, e:
            print e


