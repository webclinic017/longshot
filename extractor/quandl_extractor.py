import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from extractor.iextractor import IExtractor
load_dotenv()
QUANDL = os.getenv("QUANDL")
import pandas as pd

class QuandlExtractor(IExtractor):
    def __init__(self,ticker,start,end):
        self.ticker = ticker
        self.start = start
        self.end = end

    ## pull poly stock data by date range
    def extract(self):
        try:
            params = {'api_key':QUANDL}
            r = requests.get("https://www.quandl.com/api/v3/datasets/EOD/{}".format(self.ticker),params=params)
            price = r.json()
            return pd.DataFrame(price["dataset"]["data"],columns=price["dataset"]["column_names"])     
        except Exception as e:
            print(str(e))