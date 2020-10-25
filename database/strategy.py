from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas as pd
from datetime import timedelta
from database.abank import ABank

class Strategy(ABank):
    def __init__(self,database_name):
        super().__init__(database_name)

    def retrieve_daily_buy(self,date,tickers,delta):
        try:
            db = self.client[self.database_name]
            table = db["sim"]
            data = table.find({
                                "$and":[
                                        {"delta":{"$gte":delta}},
                                        {"delta":{"$lte":delta+0.1}}
                                        ],
                                "Date":date,
                                "ticker":{"$in":tickers},
                                },show_record_id=False).sort("delta",DESCENDING)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None
    
    def retrieve_daily_sell(self,date,tickers):
        try:
            db = self.client[self.database_name]
            table = db["sim"]
            data = table.find({
                                "Date":date,
                                "ticker":{"$in":tickers},
                                },show_record_id=False).sort("delta",DESCENDING)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None
    
    def retrieve_daily_queue_main(self,data_set,date,tickers,trade_incentive,delta,min_price):
        trade_incentive_query = {"momentum":[{"delta":{"$lte":-delta}},
                                    {"delta":{"$gte":-1}}],
                        "value":[{"delta":{"$gte":-delta}},
                                    {"delta":{"$lte":1}}]}
        trade_incentive_order = {"momentum":ASCENDING,"value":DESCENDING}
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "$and":trade_incentive_query[trade_incentive],
                                "date":date,
                                "ticker":{"$in":tickers},
                                "adjclose": {"$gte":min_price}
                                },show_record_id=False).sort("delta",trade_incentive_order[trade_incentive])
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None

    def retrieve_daily_buy_main(self,data_set,date,tickers):
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "date":date,
                                "ticker":{"$in":tickers},
                                },show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None
    
    def retrieve_daily_sell_main(self,data_set,date,tickers):
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "date":date,
                                "ticker":{"$in":tickers},
                                },show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None
    
    def retrieve_daily_queue_day(self,data_set,date,tickers,trade_incentive,delta,min_price):
        trade_incentive_query = {"momentum":[{"delta":{"$lte":-delta}},
                                    {"delta":{"$gte":-1}}],
                        "value":[{"delta":{"$gte":-delta}},
                                    {"delta":{"$lte":1}}]}
        trade_incentive_order = {"momentum":ASCENDING,"value":DESCENDING}
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "$and":trade_incentive_query[trade_incentive],
                                "Date":date,
                                "ticker":{"$in":tickers},
                                "Adj_Close": {"$gte":min_price}
                                },show_record_id=False).sort("delta",trade_incentive_order[trade_incentive])
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None
    
    def retrieve_daily_queue_day_classify(self,data_set,date,tickers,trade_incentive,min_price,accuracy):
        trade_incentive_query = {"momentum":1,
                        "value":0}
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "passed":trade_incentive_query[trade_incentive],
                                "fundamental_classification_prediction":1,
                                "price_classification_prediction":trade_incentive_query[trade_incentive],
                                "daily_classification_prediction":trade_incentive_query[trade_incentive],
                                "date":date,
                                "ticker":{"$in":tickers},
                                "adjclose": {"$gte":min_price},
                                "fundamental_accuracy":{"$gte":accuracy},
                                "price_accuracy":{"$gte":accuracy},
                                "daily_accuracy":{"$gte":accuracy}
                                },show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None

    def retrieve_daily_buy_day(self,data_set,date,tickers):
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "date":date,
                                "ticker":{"$in":tickers},
                                },show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
            return None
    
    def retrieve_daily_sell_day(self,data_set,date,tickers):
        try:
            db = self.client[self.database_name]
            table = db[data_set]
            data = table.find({
                                "date":date,
                                "ticker":{"$in":tickers},
                                },show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name,"simulation data pull",str(e))
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
    
    def retrieve_price_data(self,database,ticker):
        try:
            db = self.client[self.database_name]
            table = db[database]
            data = table.find({"ticker":ticker},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name, "sub data pull",str(e))
            return None
    
    def retrieve_sim_data(self,ticker):
        try:
            db = self.client[self.database_name]
            table = db["sim"]
            data = table.find({"ticker":ticker},show_record_id=False)
            return pd.DataFrame(list(data))
        except Exception as e:
            print(self.database_name, "sub data pull",str(e))
            return None
        