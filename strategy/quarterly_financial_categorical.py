from strategy.astrategy import AStrategy
from datetime import datetime, timedelta, timezone
from sklearn.impute import SimpleImputer
import numpy as np
import math
import pandas as pd
import pickle

class QuarterlyFinancialCategorical(AStrategy):
    def __init__(self):
        """
        Quarterly Financials Strat
        Attrs:
        year = year of model
        quarter = quarter of model
        ticker = ticker of model
        yearly_gap = number of years to project out to
        """
        super().__init__("quarterly_financial_categorical")
        self.suffix = "quarterly_financial_categorical"
    
    def transform(self,datasets,ticker,year,quarter,yearly_gap,training_years):
        """
        Required Datasets: 
            1) funds: financial data in format ["year","quarter","financial metrics"]
            2) price: price data in format ["year","quarter","price]
        """
        drop_columns = ["cik","filed","_id","adsh"]
        funds = datasets["funds"]
        for column in funds.columns:
            if str(column).islower() and str(column) != "filed":
                drop_columns.append(column)
        funds["filed"] = [datetime.strptime(str(x),"%Y%m%d").replace(tzinfo=timezone.utc) if "-" not in str(x) else \
                            datetime.strptime(str(x).split(" ")[0],"%Y-%m-%d").replace(tzinfo=timezone.utc) for x in funds["filed"]]
        funds["quarter"] = [x.quarter for x in funds["filed"]]
        funds["year"] = [x.year for x in funds["filed"]]
        price = datasets["regression"]
        classification = datasets["classification"]
        price["year"] = price["year"] - yearly_gap
        classification["year"] = classification["year"] - yearly_gap
        ticker_data = funds.merge(price,on=["year","quarter"],how="left")
        ticker_data.rename(columns={ticker:"y"},inplace=True)
        ticker_data = ticker_data.merge(classification,on=["year","quarter"],how="left")
        ticker_data.rename(columns={ticker:"y_class"},inplace=True)
        ticker_data.fillna(-99999,inplace=True)
        last = ticker_data[(ticker_data["year"] == year) & (ticker_data["quarter"] == quarter)].index.values.tolist()[0]
        ticker_data.drop(drop_columns,axis=1,inplace=True,errors="ignore")
        training_data = ticker_data.iloc[last-4*training_years:last-1]
        prediction_data = ticker_data.iloc[last]
        factors = list(prediction_data.keys())
        for key in prediction_data.keys():
            if prediction_data[key] == -99999:
                factors.remove(key)
            else:
                continue
        return training_data[factors], prediction_data[factors]

    def create_sim(self,ticker,year,yearly_gap,quarter,models,factors,data):
            """
            Required Datasets: 
                1) funds: financial data in format ["year","quarter","financial metrics"]
                2) price: price data in format ["year","quarter","price]
            """  
            ## Creating Simulation Data
            funds = data["funds"]
            price = {"ticker":ticker,"year":year+yearly_gap,"quarter":quarter}
            for i in range(len(models)):
                model = models.iloc[i]
                final = funds
                prediction = model["model"].predict(final)[0]
                if model["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]
                    prediction = p
                column_name = "{}_{}".format(self.suffix,model["model_type"])
                try:
                    price["{}_prediction".format(column_name)] = prediction[0]
                except:
                    price["{}_prediction".format(column_name)] = prediction
                price["{}_score".format(column_name)] = model["score"]
            sim = pd.DataFrame([price])
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
            column_name = "{}_{}".format(self.suffix,model["model_type"])
            price["{}_prediction".format(column_name)] = prediction[0]
            price["{}_score".format(column_name)] = model["score"]
        sim = pd.DataFrame([price])
        self.db.connect()
        self.db.store_data("rec",sim)
        self.db.close()
        return sim
    
    def store_models(self,year,quarter,sector,category,models):
        models["sector"] = sector
        models["category"] = category
        models["year"] = year
        models["quarter"] = quarter
        models["model"] = [pickle.dumps(x) for x in models["model"]]
        self.db.connect()
        self.db.store_data("models",models)
        self.db.close()
        return models
