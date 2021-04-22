import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import joblib
import math
import warnings
warnings.simplefilter(action='ignore', category=Warning)

class PortfolioSeller(object):
    @classmethod
    def sell(self,data_set,strat_db,date,portfolio):
        trades = []
        sold = []
        tickers = list(portfolio["positions"].keys())
        sell_data = strat_db.retrieve_daily_sell_day(data_set,date,tickers)
        if len(sell_data) > 0:
            for ticker in portfolio["positions"]:
                try:
                    offering = sell_data[sell_data["ticker"] == ticker]
                    if len(offering) == 1:
                        buy_price = portfolio["positions"][ticker]["buy_price"]
                        shares = portfolio["positions"][ticker]["shares"]
                        price = offering["adjclose"].iloc[0].item()
                        bench = (price - buy_price) / buy_price
                        if bench >= portfolio["delta"]:
                            money = price * shares
                            portfolio["cash"] += money
                            trade = {"ticker":ticker,
                                        "buy_date":portfolio["positions"][ticker]["buy_date"],
                                        "buy_price":buy_price,
                                        "shares": shares,
                                        "sell_date":date,
                                        "sell_price":price,
                                        "projected_delta":portfolio["positions"][ticker]["projected_delta"],
                                        "delta":bench}
                            trades.append(trade)
                            sold.append(ticker)
                        else:
                            portfolio["positions"][ticker]["price"] = price
                except Exception as e:
                    print("sell",str(e))
                    continue
        for ticker in sold:
            del portfolio["positions"][ticker] 
        return {"p":portfolio,"t":trades}
    
    @classmethod
    def sell_classify(self,data_set,strat_db,trade_incentive,date,portfolio,requiredpi):
        trades = []
        sold = []
        tickers = list(portfolio["positions"].keys())
        sell_data = strat_db.retrieve_daily_sell_day(data_set,date,tickers)
        if len(sell_data) > 0:
            for ticker in portfolio["positions"]:
                try:
                    offering = sell_data[sell_data["ticker"] == ticker]
                    if len(offering) == 1:
                        buy_price = portfolio["positions"][ticker]["buy_price"]
                        shares = portfolio["positions"][ticker]["shares"]
                        price = offering["adjclose"].iloc[0].item()
                        bench = (price - buy_price) / buy_price
                        hpr = (date - portfolio["positions"][ticker]["buy_date"]).days
                        required = ((1 + portfolio["delta"]) ** hpr) - 1
                        cutoff = -0.1
                        # if trade_incentive == "value":
                        #     if hpr > 7:
                        #         required = ((1.0002) ** hpr) - 1
                        #     if hpr > 30:
                        #         required = ((1.0001) ** hpr) - 1
                        #     if hpr > 60:
                        #         required = ((1.00005) ** hpr) - 1
                        #     if hpr > 80:
                        #         required = -0.05
                        # else:
                        #     if hpr > 5:
                        #         required = ((1.0002) ** hpr) - 1
                        #     if hpr > 7:
                        #         required = -(((1.001) ** hpr) - 1)
                        #     if hpr > 14:
                        #         required = -(((1.0025) ** hpr) - 1)
                        # sell_off = (date + timedelta(days=7)).quarter != portfolio["positions"][ticker]["buy_date"].quarter and bench < 0
                        if bench >= required or bench < cutoff:
                            money = price * shares
                            portfolio["cash"] += money
                            trade = {"ticker":ticker,
                                        "buy_date":portfolio["positions"][ticker]["buy_date"],
                                        "buy_price":buy_price,
                                        "shares": shares,
                                        "sell_date":date,
                                        "sell_price":price,
                                        "delta":bench}
                            trades.append(trade)
                            sold.append(ticker)
                        else:
                            portfolio["positions"][ticker]["price"] = price
                except Exception as e:
                    print("sell",str(e))
                    continue
        for ticker in sold:
            del portfolio["positions"][ticker] 
        return {"p":portfolio,"t":trades}