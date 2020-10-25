from pymongo import MongoClient, DESCENDING
import pandas as pd
from datetime import timedelta
from database.abank import ABank

class SEC(ABank):
    def __init__(self,database_name):
        super().__init__(database_name)

    def retrieve_sub_names(self):
        try:
            db = self.client[self.database_name]
            table = db["subs"]
            data = table.find({},{"name":1,"score":1,"ticker":1},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"pulling names",str(e))
            return None
    
    def retrieve_ciks(self):
        try:
            db = self.client[self.database_name]
            table = db["subs"]
            data = table.find({},{"cik":1,"adsh":1,"filed":1},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"pulling names",str(e))
            return None
    
    def update_sub_name(self,name,ticker,score):
        try:
            db = self.client[self.database_name]
            table = db["subs"]
            data = table.update_many({"name":name},{"$set":{"ticker":ticker,"score":score}})
        except Exception as e:
            print(self.database_name,"pulling names",str(e))
            return None

    def update_adsh_filed(self,adsh,filed):
        try:
            db = self.client[self.database_name]
            table = db["filings"]
            data = table.update_many({"adsh":adsh},{"$set":{"filed":filed}})
        except Exception as e:
            print(self.database_name,"pulling names",str(e))
            return None

    def retrieve_names(self):
        try:
            db = self.client[self.database_name]
            table = db["subs"]
            data = table.find({"score":{"$gte":80}},{"adsh":1,"name":1,"ticker":1,"score":1,"filed":1},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"pulling names",str(e))
            return None

    def retrieve_num_data(self,adsh):
        try:
            db = self.client[self.database_name]
            table = db["nums"]
            data = table.find({"adsh":adsh},{"_id":0},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"fundamental data pull",str(e))
            return None
    
    def retrieve_sub_data(self):
        try:
            db = self.client[self.database_name]
            table = db["subs"]
            data = table.find({},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name, "sub data pull",str(e))
            return None
    
    def retrieve_filing_data(self,cik):
        try:
            db = self.client[self.database_name]
            table = db["filings"]
            data = table.find({"cik":cik},show_record_id=False)
            return pd.DataFrame(list(data))
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
    
    def retrieve_hpr_industries(self,hpr):
        try:
            db = self.client[self.database_name]
            table = db["industry_categories"]
            data = table.find({"hpr":hpr
                                },{"industry":1},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None