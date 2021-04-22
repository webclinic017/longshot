import pandas as pd
from datetime import datetime, timedelta
import os
import math
import warnings
warnings.simplefilter(action='ignore', category=Warning)

class PortfolioQueue(object):
    @classmethod
    def queue(self,tickers,strat,d,portfolio):
        try:
            ##queue
            holdings = portfolio["positions"]
            capital = portfolio["cash"] + sum([holdings[x]["price"] * holdings[x]["shares"] for x in holdings])            
            portfolio_tickers = []
            data = strat.retrieve_daily_buy(d,list(tickers),portfolio["required"])
            if(len(data)) > 0:
                offerings = data.sort_values("delta",ascending=False)
                required_cash = capital * portfolio["weight"]
                seats = math.floor(min(len(offerings),portfolio["cash"]/required_cash))
                if seats > 0:
                    for i in range(seats):
                        try:
                            portfolio_tickers.extend(list(portfolio["positions"].keys()))
                            portfolio_tickers.extend(list([p["ticker"] for p in portfolio["queue"]]))
                            offering = offerings.iloc[i]
                            if offering["ticker"] not in portfolio["queue"] and offering["ticker"] not in portfolio_tickers:
                                portfolio["queue"].append({"ticker":offering["ticker"],"amount":required_cash,"projected_delta":offering["delta"]})
                        except Exception as e:
                            print("offerings", str(e))
                            continue
            portfolio["date"] = d
            return portfolio
        except Exception as e:
            message = {"status":"queue","msg":str(e)}
            print(message)
    
    @classmethod
    def queue_price_ll(self,tickers,strat,d,portfolio,ll):
        try:
            ##queue
            holdings = portfolio["positions"]
            capital = portfolio["cash"] + sum([holdings[x]["price"] * holdings[x]["shares"] for x in holdings])            
            portfolio_tickers = []
            data = strat.retrieve_daily_buy_ll(d,list(tickers),portfolio["required"],ll)
            if(len(data)) > 0:
                offerings = data.sort_values("delta",ascending=False)
                required_cash = capital * portfolio["weight"]
                seats = math.floor(min(len(offerings),portfolio["cash"]/required_cash))
                if seats > 0:
                    for i in range(seats):
                        try:
                            portfolio_tickers.extend(list(portfolio["positions"].keys()))
                            portfolio_tickers.extend(list([p["ticker"] for p in portfolio["queue"]]))
                            offering = offerings.iloc[i]
                            if offering["ticker"] not in portfolio["queue"] and offering["ticker"] not in portfolio_tickers:
                                portfolio["queue"].append({"ticker":offering["ticker"],"amount":required_cash,"projected_delta":offering["delta"]})
                        except Exception as e:
                            print("offerings", str(e))
                            continue
            portfolio["date"] = d
            return portfolio
        except Exception as e:
            message = {"status":"queue","msg":str(e)}
            print(message)
    
    @classmethod
    def queue_diversified(self,tickers,strat,d,portfolio):
        try:
            ##queue
            ll = portfolio["ll"]
            holdings = portfolio["positions"]
            capital = portfolio["cash"] + sum([holdings[x]["price"] * holdings[x]["shares"] for x in holdings])            
            portfolio_tickers = []
            data = strat.retrieve_daily_buy_ll(d,list(tickers),portfolio["required"],ll)
            if(len(data)) > 0:
                offerings = data.sort_values("delta",ascending=False)
                required_cash = capital * portfolio["weight"]
                seats = math.floor(min(len(offerings),portfolio["cash"]/required_cash))
                if seats > 0:
                    for i in range(seats):
                        try:
                            portfolio_tickers.extend(list(portfolio["positions"].keys()))
                            portfolio_tickers.extend(list([p["ticker"] for p in portfolio["queue"]]))
                            offering = offerings.iloc[i]
                            if offering["ticker"] not in portfolio["queue"] and offering["ticker"] not in portfolio_tickers:
                                portfolio["queue"].append({"ticker":offering["ticker"],"amount":required_cash,"projected_delta":offering["delta"]})
                        except Exception as e:
                            print("offerings", str(e))
                            continue
            portfolio["date"] = d
            return portfolio
        except Exception as e:
            message = {"status":"queue","msg":str(e)}
            print(message)
    
    @classmethod
    def queue_main(self,data_set,tickers,strat,d,portfolio):
        try:
            ##queue
            ll = portfolio["ll"]
            holdings = portfolio["positions"]
            capital = portfolio["cash"] + sum([holdings[x]["price"] * holdings[x]["shares"] for x in holdings])            
            portfolio_tickers = []
            data = strat.retrieve_daily_buy_ll(d,list(tickers),portfolio["required"],ll)
            if(len(data)) > 0:
                offerings = data.sort_values("delta",ascending=False)
                required_cash = capital * portfolio["weight"]
                seats = math.floor(min(len(offerings),portfolio["cash"]/required_cash))
                if seats > 0:
                    for i in range(seats):
                        try:
                            portfolio_tickers.extend(list(portfolio["positions"].keys()))
                            portfolio_tickers.extend(list([p["ticker"] for p in portfolio["queue"]]))
                            offering = offerings.iloc[i]
                            if offering["ticker"] not in portfolio["queue"] and offering["ticker"] not in portfolio_tickers:
                                portfolio["queue"].append({"ticker":offering["ticker"],"amount":required_cash,"projected_delta":offering["delta"]})
                        except Exception as e:
                            print("offerings", str(e))
                            continue
            portfolio["date"] = d
            return portfolio
        except Exception as e:
            message = {"status":"queue","msg":str(e)}
            print(message)