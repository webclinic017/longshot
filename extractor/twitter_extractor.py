import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from extractor.iextractor import IExtractor
load_dotenv()
TWITTERBEARER = os.getenv("TWITTERBEARER")
import pandas as pd

class TwitterExtractor(IExtractor):
    def __init__(self,company,start,end):
        self.company = company
        self.start = start
        self.end = end

    ## pull poly stock data by date range
    def extract(self):
        try:
            headers = {
                "Authorization": "Bearer {}".format(TWITTERBEARER)
            }
            params = {
                        "query": self.company,
                        "start_time": self.start,
                        "end_time": self.end,
                        "max_results":500,}
            r = requests.get("https://api.twitter.com/2/tweets/search/all",headers=headers,params=params)
            return r.json()
        except Exception as e:
            print(str(e))