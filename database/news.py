from pymongo import MongoClient, DESCENDING
import pandas as pd
from datetime import timedelta
from bank.abank import ABank

class News(ABank):
    def __init__(self,database_name):
        super().__init__(database_name)
    
    def retrieve_news_decade(self,year):
        try:
            db = self.client[self.database_name]
            table = db["nyt"]
            data = table.find({"date":{"$gte":datetime(year,1,1),
                                        "$lte":datetime(year+10,1,1)}},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"pulling names",str(e))
            return None