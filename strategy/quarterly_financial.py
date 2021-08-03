from strategy.astrategy import AStrategy
from datetime import datetime, timedelta, timezone
from sklearn.impute import SimpleImputer
import numpy as np
import math
import pandas as pd
import pickle

class QuarterlyFinancial(AStrategy):
    def __init__(self,year,quarter,ticker,yearly_gap,training_years):
        """
        Quarterly Financials Strat
        Attrs:
        year = year of model
        quarter = quarter of model
        ticker = ticker of model
        yearly_gap = number of years to project out to
        """
        super().__init__("quarterly_financial")
        self.year = year
        self.quarter = quarter
        self.ticker = ticker
        self.yearly_gap = yearly_gap
        self.training_years = training_years
    
    def transform(self,datasets):
        """
        Required Datasets: 
            1) funds: financial data in format ["year","quarter","financial metrics"]
            2) price: price data in format ["year","quarter","price]
        """
        drop_columns = ["cik","filed","_id","adsh"]
        funds = datasets["funds"]
        funds["filed"] = [datetime.strptime(str(x),"%Y%m%d").replace(tzinfo=timezone.utc) if "-" not in str(x) else \
                            datetime.strptime(str(x).split(" ")[0],"%Y-%m-%d").replace(tzinfo=timezone.utc) for x in funds["filed"]]
        funds["quarter"] = [x.quarter for x in funds["filed"]]
        funds["year"] = [x.year for x in funds["filed"]]
        price = datasets["regression"]
        classification = datasets["classification"]
        ticker_data = funds.merge(price,on=["year","quarter"],how="left")
        ticker_data.rename(columns={self.ticker:"y"},inplace=True)
        ticker_data = ticker_data.merge(classification,on=["year","quarter"],how="left")
        ticker_data.rename(columns={self.ticker:"y_class"},inplace=True)
        ticker_data.fillna(-99999,inplace=True)
        last = ticker_data[(ticker_data["year"] == self.year) & (ticker_data["quarter"] == self.quarter)].index.values.tolist()[0]
        ticker_data.drop(drop_columns,axis=1,inplace=True,errors="ignore")
        training_data = ticker_data.iloc[last-4*self.training_years:last-1]
        prediction_data = ticker_data.iloc[last]
        factors = list(prediction_data.keys())
        for key in prediction_data.keys():
            if prediction_data[key] == -99999:
                factors.remove(key)
            else:
                continue
        return training_data[factors], prediction_data[factors]

    def create_sim(self,models,factors,data):
            """
            Required Datasets: 
                1) funds: financial data in format ["year","quarter","financial metrics"]
                2) price: price data in format ["year","quarter","price]
            """  
            ## Creating Simulation Data
            funds = data["funds"]
            price = {"ticker":self.ticker,"year":self.year+self.yearly_gap,"quarter":self.quarter}
            for i in range(len(models)):
                model = models.iloc[i]
                final = funds[(funds["year"] == self.year) & (funds["quarter"] == self.quarter)][factors]
                prediction = model["model"].predict(final)[0]
                if model["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]
                    prediction = p
                column_name = "quarterly_fundamental_{}".format(model["model_type"])
                price["{}_prediction".format(column_name)] = prediction[0]
                price["{}_score".format(column_name)] = model["score"]
            sim = pd.DataFrame([price])
            self.db.connect()
            self.db.store_data("sim",sim)
            self.db.close()
            return sim
    
    ### This needs debugging
    def create_rec(self,models,factors,data):
        """
        Required Datasets: 
            1) funds: financial data in format ["year","quarter","financial metrics"]
            2) price: price data in format ["year","quarter","price]
        """  
        ## Creating Simulation Data
        funds = data["funds"]
        price = {"ticker":self.ticker,"year":self.year+self.yearly_gap,"quarter":self.quarter}
        for i in range(len(models)):
            model = models.iloc[i]
            final = funds[(funds["year"] == self.year) & (funds["quarter"] == self.quarter)]
            prediction = model["model"].predict(final)[0]
            if model["api"] == "tf":
                p = []
                [p.extend(x) for x in prediction]
                prediction = p
            column_name = "quarterly_fundamental_{}".format(model["model_type"])
            price["{}_prediction".format(column_name)] = prediction[0]
            price["{}_score".format(column_name)] = model["score"]
        sim = pd.DataFrame([price])
        self.db.connect()
        self.db.store_data("rec",sim)
        self.db.close()
        return sim

    def backtest(self,epoch_dict,sim):
        return "lel"
    
    def store_models(self,models):
        models["ticker"] = self.ticker
        models["year"] = self.year
        models["quarter"] = self.quarter
        models["model"] = [pickle.dumps(x) for x in models["model"]]
        self.db.connect()
        self.db.store_data("models",models)
        self.db.close()
        return models
