from pymongo import MongoClient, DESCENDING
import pandas as pd
from datetime import timedelta
from database.abank import ABank

class Market(ABank):
    def __init__(self):
        super().__init__("market")
  
    def retrieve_price_data(self,database,ticker):
        try:
            db = self.client[self.database_name]
            table = db[database]
            data = table.find({"ticker":ticker},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name, "sub data pull",str(e))
            return None
        
    def retrieve_tickers(self,database):
        try:
            db = self.client[self.database_name]
            table = db[database]
            data = table.distinct("ticker")
            return data
        except Exception as e:
            print(self.database_name, "sub data pull",str(e))
            return None
    
    def retrieve_industry_tickers(self,industry):
        try:
            db = self.client[self.database_name]
            table = db["sp500"]
            data = table.find({"GICS Sector":industry
                                },{"Symbol":1},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None
    
    def retrieve_price_date(self,database,date):
        try:
            db = self.client[self.database_name]
            table = db[database]
            data = table.find({"Date":{"$gte":date}},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name, "sub data pull",str(e))
            return None
    
    def retrieve_training_data(self,data_set,start_date,end_date):
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "$and":[{"date":{"$gte":start_date}},
                                        {"date":{"$lte":end_date}}
                                ],
                                },show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None