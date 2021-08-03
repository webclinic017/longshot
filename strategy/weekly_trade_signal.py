from strategy.astrategy import AStrategy
from datetime import datetime, timedelta, timezone
from sklearn.impute import SimpleImputer
import numpy as np
import math
import pandas as pd
import pickle

class WeeklyTradeSignal(AStrategy):
    def __init__(self):
        """
        Quarterly Financials Strat
        Attrs:
        year = year of model
        quarter = quarter of model
        ticker = ticker of model
        yearly_gap = number of years to project out to
        """
        super().__init__("weekly_trade_signal")
        self.suffix = "trade_signal"
    
    def transform(self,datasets,year,quarter,ticker,week_gap,training_years):
        """
        Required Datasets: 
            1) funds: financial data in format ["year","quarter","financial metrics"]
            2) price: price data in format ["year","quarter","price]
        """

        ticker_data = datasets["ticker_data"]
        ticker_data["rolling_100"] = ticker_data["adjclose"].rolling(window=100).mean()
        ticker_data["rolling_7"] = ticker_data["adjclose"].rolling(window=5).mean()
        ticker_data.dropna(inplace=True)
        wmtd = []
        for i in range(5,ticker_data.index.size):
            values = ticker_data.iloc[i-5:i]["adjclose"]
            max_price = max(values)
            wmtd.append(max_price)
        ticker_data = ticker_data.iloc[5:]
        ticker_data["wmtd"] = wmtd
        wtd = ticker_data.groupby(["year","week","ticker"]).mean()
        wtd.reset_index(inplace=True)
        wtd["y"] = wtd["adjclose"].shift(-week_gap)
        wtd["y_class"] = [1 if x > 0 else 0 for x in wtd["adjclose"].diff()]
        ticker_regression = wtd.iloc[:-week_gap]
        first = ticker_regression[(ticker_regression["year"] == year - training_years) & (ticker_regression["quarter"] == quarter)].index.values.tolist()[0]
        last = ticker_regression[(ticker_regression["year"] == year) & (ticker_regression["quarter"] == quarter)].index.values.tolist()[0]
        training_data = ticker_regression.iloc[first:last-1]
        prediction_data = ticker_regression[(ticker_regression["year"] == year) & (ticker_regression["quarter"] == quarter)]
        factors = prediction_data.columns
        ff = []
        for factor in factors:
            if True in [x == -99999 for x in prediction_data[factor]]:
                continue
            else:
                ff.append(factor)
        return training_data[ff], prediction_data[ff]

    def create_sim(self,year,quarter,ticker,models,factors,data):
            """
            Required Datasets: 
                1) funds: financial data in format ["year","quarter","financial metrics"]
                2) price: price data in format ["year","quarter","price]
            """  
            ## Creating Simulation Data
            current_sets = []
            prediction_set = data["prediction_set"]
            price_dict = []
            for week in prediction_set["week"]:
                price_dict.append({"year":year,"quarter":quarter,"week":week})
            price = pd.DataFrame(price_dict)
            for j in range(len(models)):
                model = models.iloc[j]
                price_model = model["model"]
                prediction = price_model.predict(prediction_set[factors])
                current_set = price.copy()
                col_name = model["model_type"]
                current_set["{}_{}_prediction".format(self.suffix,col_name)] = prediction
                current_set["{}_{}_score".format(self.suffix,col_name)] = model["score"]
                current_sets.append(current_set)
            sim = current_sets[0]
            for cs in current_sets[1:]:
                sim = sim.merge(cs,on=["year","quarter","week"],how="left")
            sim["ticker"] = ticker
            return sim

    ### This needs debugging
    def create_rec(self,models,factors,data):
        """
        Required Datasets: 
            1) funds: financial data in format ["year","quarter","financial metrics"]
            2) price: price data in format ["year","quarter","price]
        """  
        ## Creating Simulation Data
        # funds = data["funds"]
        # price = {"ticker":self.ticker,"year":self.year+self.yearly_gap,"quarter":self.quarter}
        # for i in range(len(models)):
        #     model = models.iloc[i]
        #     final = funds[(funds["year"] == self.year) & (funds["quarter"] == self.quarter)]
        #     prediction = model["model"].predict(final)[0]
        #     if model["api"] == "tf":
        #         p = []
        #         [p.extend(x) for x in prediction]
        #         prediction = p
        #     column_name = "quarterly_fundamental_{}".format(model["model_type"])
        #     price["{}_prediction".format(column_name)] = prediction
        #     price["{}_score".format(column_name)] = model["score"]
        # sim = pd.DataFrame([price])
        # self.db.connect()
        # self.db.store_data("rec",sim)
        # self.db.close()
        return "lel"

