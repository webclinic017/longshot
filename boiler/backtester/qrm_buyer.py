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
    def buy(self,strat,d,portfolio):
        try:
            ##buy
            tickers = [x["ticker"] for x in portfolio["queue"]]
            data = strat.retrieve_daily_buy(d,list(tickers),portfolio["required"])
            if len(data) > 0:
                for i in range(min(len(portfolio["queue"]),len(data))):
                    ticker = portfolio["queue"][i]["ticker"]
                    required_cash = portfolio["queue"][i]["amount"]
                    projected_delta = portfolio["queue"][i]["projected_delta"]
                    offering = data[data["ticker"] == ticker]
                    if len(offering) == 1: 
                        price = offering["Adj_Close"].iloc[0].item()
                        if portfolio["cash"] >= required_cash:
                            shares = required_cash / price
                            portfolio["cash"] -= required_cash
                            portfolio["positions"][ticker] = {"shares":shares,"projected_delta":projected_delta,"buy_price":price,"price":price,"buy_date":d}
            portfolio["queue"] = []
            portfolio["date"] = d
            return portfolio
        except Exception as e:
            message = {"status":"buy","msg":str(e)}
            print(message)