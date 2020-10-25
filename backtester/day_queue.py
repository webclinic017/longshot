import pandas as pd
from datetime import datetime, timedelta
import os
import math
import warnings
from statistics import mean
warnings.simplefilter(action='ignore', category=Warning)

class PortfolioQueue(object):  
    @classmethod
    def queue_price(self,data_set,strat_db,trade_incentive,tickers,date,portfolio):
        try:
            ##queue
            holdings = portfolio["positions"]
            capital = portfolio["cash"] + sum([holdings[x]["price"] * holdings[x]["shares"] for x in holdings])            
            portfolio_tickers = []
            data = strat_db.retrieve_daily_queue_day(data_set,date,list(tickers),trade_incentive,portfolio["delta"],portfolio["min_price"])
            if(len(data)) > 0:
                offerings = data
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
            portfolio["date"] = date
            return portfolio
        except Exception as e:
            message = {"status":"queue","msg":str(e)}
            print(message)

    @classmethod
    def queue_classify(self,data_set,strat_db,trade_incentive,asc,correlation_value,tickers,date,portfolio,required):
        try:
            ##queue
            buy_months = {"momentum":[3,8,12],"value":[2,3,12]}
            holdings = portfolio["positions"]
            capital = portfolio["cash"] + sum([holdings[x]["price"] * holdings[x]["shares"] for x in holdings])            
            portfolio_tickers = []
            data = strat_db.retrieve_daily_queue_day_classify(data_set,date,list(tickers),trade_incentive,portfolio["min_price"],portfolio["accuracy"])
            correlations = strat_db.retrieve_data("correlations")
            # buy_period = date.month not in buy_months[trade_incentive]
            if(len(data)) > 0:
                offerings = data
                offerings.sort_values("adjclose",ascending=asc,inplace=True)
                required_cash = capital * portfolio["weight"]
                seats = math.floor(min(len(offerings),portfolio["cash"]/required_cash))
                if seats > 0:
                    for i in range(seats):
                        try:
                            portfolio_tickers.extend(list(portfolio["positions"].keys()))
                            portfolio_tickers.extend(list([p["ticker"] for p in portfolio["queue"]]))
                            offering = offerings.iloc[i]
                            correlated = False
                            correlation = correlations[(correlations["ticker"] == offering["ticker"]) & (correlations["ticker_2"].isin(portfolio_tickers))]["daily_correlation"]
                            if len(correlation) == 0:
                                correlated = False
                            else:
                                abs_correlation = [abs(x) for x in correlation]
                                correlated = mean(abs_correlation) > correlation_value
                            if offering["ticker"] not in portfolio["queue"] \
                                    and offering["ticker"] not in portfolio_tickers \
                                    and not correlated:
                                portfolio["queue"].append({"ticker":offering["ticker"],"amount":required_cash})
                        except Exception as e:
                            print("offerings", str(e))
                            continue
            portfolio["date"] = date
            return portfolio
        except Exception as e:
            message = {"status":"queue","msg":str(e)}
            print(message)