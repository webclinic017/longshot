import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import joblib
import math
import warnings
warnings.simplefilter(action='ignore', category=Warning)

class PortfolioBuyer(object):
    @classmethod
    def buy(self,data_set,strat_db,date,portfolio):
        ##buy
        tickers = [x["ticker"] for x in portfolio["queue"]]
        data = strat_db.retrieve_daily_buy_day(data_set,date,list(tickers))
        if len(data) > 0:
            for i in range(min(len(portfolio["queue"]),len(data))):
                try:
                    ticker = portfolio["queue"][i]["ticker"]
                    required_cash = portfolio["queue"][i]["amount"]
                    projected_delta = portfolio["queue"][i]["projected_delta"]
                    offering = data[data["ticker"] == ticker]
                    if len(offering) == 1: 
                        price = offering["Adj_Close"].iloc[0].item()
                        if portfolio["cash"] >= required_cash:
                            shares = required_cash / price
                            portfolio["cash"] -= required_cash
                            portfolio["positions"][ticker] = {"shares":shares,"projected_delta":projected_delta
                            ,"buy_price":price,"price":price,"buy_date":date}
                except Exception as e:
                    message = {"status":"buy","msg":str(e)}
                    print(message)
        portfolio["queue"] = []
        portfolio["date"] = date
        return portfolio

    @classmethod
    def buy_classify(self,data_set,strat_db,trade_incentive,date,portfolio):
        ##buy
        buy_months = {"momentum":[3,8,12],"value":[2,3,12]}
        buy_period = date.month not in buy_months[trade_incentive]
        tickers = [x["ticker"] for x in portfolio["queue"]]
        data = strat_db.retrieve_daily_buy_day(data_set,date,list(tickers))
        if len(data) > 0 and buy_period:
            for i in range(min(len(portfolio["queue"]),len(data))):
                try:
                    ticker = portfolio["queue"][i]["ticker"]
                    required_cash = portfolio["queue"][i]["amount"]
                    offering = data[data["ticker"] == ticker]
                    if len(offering) == 1: 
                        price = offering["adjclose"].iloc[0].item()
                        if portfolio["cash"] >= required_cash:
                            shares = required_cash / price
                            portfolio["cash"] -= required_cash
                            portfolio["positions"][ticker] = {"shares":shares
                            ,"buy_price":price,"price":price,"buy_date":date}
                except Exception as e:
                    message = {"status":"buy","msg":str(e)}
                    print(message)
        portfolio["queue"] = []
        portfolio["date"] = date
        return portfolio