from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
import os
load_dotenv()
token = os.getenv("TIINGO")
class TiingoExtractor(object):
    def __init__(self,ticker,start,end):
        self.ticker = ticker
        self.start = start
        self.end = end

    def extract(self):
        try:
            headers = {
                'Content-Type': 'application/json'
            }
            params = {
                "token":token,
                "startDate":self.start,
                "endDate":self.end,  
            }
            url = "https://api.tiingo.com/tiingo/daily/{}/prices".format(self.ticker)
            requestResponse = requests.get(url,headers=headers,params=params)
            return pd.DataFrame(requestResponse.json())
        except Exception as e:
            print(str(e))