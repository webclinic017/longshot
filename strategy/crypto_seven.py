from strategy.astrategy import AStrategy
from datetime import datetime, timedelta, timezone
from sklearn.impute import SimpleImputer
import numpy as np
import math
import pandas as pd
import pickle

class CryptoSeven(AStrategy):
    def __init__(self):
        super().__init__("crypto")
        self.suffix = "crypto"
    
    def transform(self,data,year,week,daily_gap,training_days):
        """
        Required Datasets: 
            1) funds: financial data in format ["year","quarter","financial metrics"]
            2) price: price data in format ["year","quarter","price]
        """
        data["y"] = data["closing_price_"].shift(-daily_gap)
        data["y_class"] = [1 if x > 0 else 0 for x in data["closing_price_"].diff()]
        data["y_class"] = data["y_class"].shift(-daily_gap)
        last = data[(data["year"] == year) & (data["week"]==week)].index.values.tolist()[0]
        training_data = data.iloc[last-training_days-1:last-1]
        prediction_data = data[(data["year"] == year) & (data["week"]==week)]
        return training_data, prediction_data

    def create_sim(self,dataset,models,factors,prediction_data):
            """
            Required Datasets: 
                1) funds: financial data in format ["year","quarter","financial metrics"]
                2) price: price data in format ["year","quarter","price]
            """  
            ## Creating Simulation Data
            price_dict= []
            for date in prediction_data["date"]:
                price_dict.append({"date":date})
            price = pd.DataFrame(price_dict)
            current_sets = []
            for j in range(len(models)):
                model = models.iloc[j]
                price_model = model["model"]
                prediction = price_model.predict(prediction_data[factors])
                current_set = price.copy()
                col_name = model["model_type"]
                current_set["{}_{}_prediction".format("crypto",col_name)] = prediction
                current_set["{}_{}_score".format("crypto",col_name)] = model["score"]
                current_sets.append(current_set)
            sim = current_sets[0]
            for cs in current_sets[1:]:
                sim = sim.merge(cs,on="date",how="left")
            sim["currency"] = dataset
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
        # funds = data["funds"]
        # price = {"currency":self.currency,"year":self.year+self.yearly_gap,"quarter":self.quarter}
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

    def backtest(self,start,end,sim,seats,ceiling,weekly_value,delta_req,trade_signal_score):
        trades = []
        blacklist = pd.DataFrame([{"currency":"ZZZZZ","start":datetime(2016,4,1),"end":datetime(2016,4,14)}])
        daily_delta_req = float(np.log(1+delta_req)/365)
        delta_column = "predicted_delta"
        hpr = 2
        sim["delta"] = [abs(x) for x in sim[delta_column]]
        if not weekly_value:
            sim[delta_column] = sim[delta_column] * -1
        if ceiling:
            for col in sim.columns:
                    if 'delta' in col:
                        sim["{}_abs".format(col)] = [abs(x) for x in sim[col]]
                        filtered_sim = sim[(sim["{}_abs".format(col)] <= 1)]
        else:
            filtered_sim = sim
        filtered_sim = filtered_sim[(filtered_sim[delta_column] >= float((1+daily_delta_req)**hpr) - 1)]
        filtered_sim = filtered_sim[filtered_sim[f"{self.suffix}_classification_prediction"] == 1]
        filtered_sim = filtered_sim[filtered_sim[f"{self.suffix}_classification_score"] >= trade_signal_score]
        for seat in range(seats):
            date = start
            while date <= end:
                if date >= end:
                    break
                if date.weekday() > 4 \
                        or date == datetime(2020,2,20):
                    date = date + timedelta(days=7-date.weekday())
                blacklist_currencys = blacklist[(blacklist["start"] <= date) & (blacklist["end"] >= date)]["currency"]
                todays_sim = filtered_sim[ (~filtered_sim["currency"].isin(blacklist_currencys)) & \
                                          (filtered_sim["date"] == date)]
                if todays_sim.index.size >= 1:
                    offerings = todays_sim.sort_values(delta_column,ascending=False)
                    for offering in range(offerings.index.size):
                        try:
                            trade_currency = offerings.iloc[offering]["currency"]
                            try:
                                trade = sim[(sim["currency"] == trade_currency) & (sim["date"] > date)].iloc[0]
                                sell_date = trade["date"] + timedelta(days=1)
                                sell_trades = sim[(sim["date"] >= sell_date) & (sim["currency"] == trade["currency"])]
                                
                                ## if there aren't any listed prices left for the currency
                                if sell_trades.index.size < 1:
                                    ## if there aren't any more currency's left to vet
                                    if offering == offerings.index.size - 1:
                                        date = sell_date + timedelta(days=1)
                                        print(date,"no more stock vets")
                                        break
                                    else:
                                        print(date,"stock had no more listed prices")
                                        continue
                                else:
                                    if date > datetime(2020,10,1):
                                        max_hpr = int((end - date).days) - 1
                                    else:
                                        max_hpr = hpr
                                    sell_trades["gain"] = (sell_trades["closing_price_"] - trade["closing_price_"]) / trade["closing_price_"]
                                    sell_trades["hpr"] = [(x - trade["date"]).days for x in sell_trades["date"]]
                                    sell_trades["delta_req"] = [float((1+daily_delta_req) ** (math.floor(x/7)*7) - 1) for x in sell_trades["hpr"]]
                                    ## trades within target holding period
                                    hpr_sell_trades = sell_trades[sell_trades["hpr"] <= max_hpr]
                                    hpr_sell_trades["hit"] = hpr_sell_trades["gain"] >= hpr_sell_trades["delta_req"]
                                    
                                    ## trades within delta requirement
                                    delta_hit = hpr_sell_trades[(hpr_sell_trades["hit"] == True)]
                                    
                                    ## trades not within delta requirement
                                    if delta_hit.index.size < 1:
                                        exits = sell_trades[(sell_trades["closing_price_"] >= trade["closing_price_"]) & (sell_trades["hpr"]>max_hpr)]
                                        if exits.index.size < 1:
                                            yearly_trades = sell_trades[sell_trades["year"]==date.year]
                                            sell_trade = yearly_trades.iloc[yearly_trades.index.size - 1]
                                        else:
                                            sell_trade = exits.iloc[0]
                                    else:
                                        sell_trade = delta_hit.iloc[0]
                                    trade["sell_price"] = sell_trade["closing_price_"]
                                    trade["delta_req"] = sell_trade["delta_req"]
                                    trade["sell_date"] = sell_trade["date"]
                                    trade["sell_delta"] = (sell_trade["closing_price_"] - trade["closing_price_"]) / trade["closing_price_"]
                                    trade["hpr"] = sell_trade["hpr"]
                                    trade["seat"] = seat
                                    blacklist = blacklist.append([{"currency":trade["currency"],"start":date,"end":trade["sell_date"]}])
                                    trades.append(trade)
                                    date = trade["sell_date"] + timedelta(days=1)
                                    break
                            except Exception as e:
                                print(date,str(e))
                                date = date + timedelta(days=1)
                                continue
                        except Exception as e:
                            print(date,str(e))
                            date = date + timedelta(days=1)
                            continue
                else:
                    date = date + timedelta(days=1)
                    continue
        return trades
