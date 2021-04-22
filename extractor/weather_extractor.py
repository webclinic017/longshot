import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from extractor.iextractor import IExtractor
load_dotenv()
WEATHER = os.getenv("WEATHER")
import pandas as pd

class WeatherExtractor(IExtractor):
    def __init__(self,location,start,end):
        self.location = location
        self.start = start
        self.end = end

    ## pull poly stock data by date range
    def extract(self):
        try:
            params = {
                        "q": self.location,
                        "key": WEATHER,
                        "tp": 24,
                        "date": self.start,
                        "enddate": self.end,
                        "format":  "json"}
            r = requests.get("https://api.worldweatheronline.com/premium/v1/past-weather.ashx",params=params)
            return r.json()
        except Exception as e:
            print(str(e))