import pandas as pd
from strategy.istrategy import IStrategy
from database.strategy import Strategy
## https://realpython.com/python-super/
class AStrategy(IStrategy):

    def __init__(self,name):
        super().__init__()
        self.name = name
        self.db = Strategy(name)
    
    def transform(self,datasets,classification):
        return datasets
    
    def create_sim(self,models,data):
        result = pd.DataFrame([{"test":test}])
        self.db.connect()
        self.db.store_data("sim",result)
        self.db.close()
        return result
    
    def create_rec(self,models,data):
        result = pd.DataFrame([{"test":test}])
        self.db.connect()
        self.db.store_data("recs",result)
        self.db.close()
        return result
    
    def backtest(self,epoch_dict,sim):
        result = pd.DataFrame([{"test":test}])
        self.db.connect()
        self.db.store_data("trades",result)
        self.db.store_data("epochs",pd.DataFrame([epoch_dict]))
        self.db.close()
        return result
    
    def reset_db(self):
        self.db.connect()
        self.db.reset()
        self.db.close()

    def retrieve_data(self,table_name):
        self.db.connect()
        data = self.db.retrieve_data(table_name)
        self.db.close()
        return data
    
    def store_models(self,models,ticker,year,quarter):
        models["ticker"] = ticker
        models["year"] = year
        models["quarter"] = quarter
        models["model"] = [pickle.dumps(x) for x in models["model"]]
        self.db.connect()
        self.db.store_data("models",models)
        self.db.close()
        return models
    
    def clear_trades(self):
        try:
            self.db.connect()
            db = self.db.client[self.db.database_name]
            collections = db.list_collection_names()
            for collection in collections:
                if "trades" in collection:
                    table = db[collection]
                    table.drop()
            self.db.close()
        except Exception as e:
            print(self.db.database_name,str(e))
            return None  
