import requests as r
from dotenv import load_dotenv
import os
load_dotenv()
NYT = os.getenv("NYT")


class NYTExtractor(object):

    @classmethod
    def query(self,year,month):
        headlines = r.get("https://api.nytimes.com/svc/archive/v1/{}/{}.json?api-key={}".format(year,month,NYT))
        docs = headlines.json()
        return docs