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
    def sell(self,strat,date,portfolio):
        trades = []
        sold = []
        tickers = list(portfolio["positions"].keys())
        sell_data = strat.retrieve_daily_sell(date,tickers)
        if len(sell_data) > 0:
            for ticker in portfolio["positions"]:
                try:
                    offering = sell_data[sell_data["ticker"] == ticker]
                    if len(offering) == 1:
                        buy_price = portfolio["positions"][ticker]["buy_price"]
                        shares = portfolio["positions"][ticker]["shares"]
                        price = offering["Adj_Close"].iloc[0].item()
                        bench = (price - buy_price) / buy_price
                        if bench >= portfolio["required"]:
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
    def idgaf_sell(self,strat,date,portfolio):
        trades = []
        sold = []
        tickers = list(portfolio["positions"].keys())
        sell_data = strat.retrieve_daily_sell(date,tickers)
        if len(sell_data) > 0:
            for ticker in portfolio["positions"]:
                try:
                    offering = sell_data[sell_data["ticker"] == ticker]
                    if len(offering) == 1:
                        buy_price = portfolio["positions"][ticker]["buy_price"]
                        shares = portfolio["positions"][ticker]["shares"]
                        price = offering["Adj_Close"].iloc[0].item()
                        bench = (price - buy_price) / buy_price
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
                except Exception as e:
                    print("sell",str(e))
                    continue
        for ticker in sold:
            del portfolio["positions"][ticker] 
        return {"p":portfolio,"t":trades}   