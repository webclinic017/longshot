from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import math

class Backtester(object):
    
    @classmethod
    def backtest(self,start,end,sim,seats,extreme,ceiling,fixed,classification,value,quarterly_delta_req,weekly_delta_req,trade_signal_score):
        trades = []
        quarterly_hpr = 30
        weekly_hpr = 7
        blacklist = pd.DataFrame([{"ticker":"ZZZZZ","start":datetime(2016,4,1),"end":datetime(2016,4,14)}])
        daily_delta_req = float(np.log(1+quarterly_delta_req)/weekly_hpr)
        if not value:
            for col in sim.columns:
                if "delta" in col:
                    sim[col] = sim[col] * -1
            if extreme and classification:
                for col in sim.columns:
                    if "classification_prediction" in col:
                        sim[col] = [0 if x == 1 else 1 for x in sim[col]]
        if ceiling:
            for col in sim.columns:
                    if 'delta' in col:
                        sim["{}_abs".format(col)] = [abs(x) for x in sim[col]]
                        filtered_sim = sim[(sim["{}_abs".format(col)] <= 1)]
        else:
            filtered_sim = sim
        for col in sim.columns:
            if "regression_score" in col:
                filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
        if not fixed:
            filtered_sim = filtered_sim[(filtered_sim["quarterly_predicted_delta"] >= float((1+daily_delta_req)**quarterly_hpr) - 1)]
            filtered_sim = filtered_sim[(filtered_sim["weekly_predicted_delta"] >= float((1+daily_delta_req)**weekly_hpr) - 1)]
        else:
            filtered_sim = filtered_sim[(filtered_sim["weekly_predicted_delta"] >= float(weekly_delta_req))]
            filtered_sim = filtered_sim[(filtered_sim["quarterly_predicted_delta"] >= float(quarterly_delta_req))]
        if classification:
            for col in sim.columns:
                if "classification_score" in col:
                    filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
                if "classification_prediction" in col:
                    filtered_sim = filtered_sim[filtered_sim[col] == 1]
        for seat in range(seats):
            date = start
            while date <= end:
                if date >= end:
                    break
                if date.weekday() > 4 \
                        or date == datetime(2020,2,20):
                    date = date + timedelta(days=7-date.weekday())
                blacklist_tickers = blacklist[(blacklist["start"] <= date) & (blacklist["end"] >= date)]["ticker"]
                todays_sim = filtered_sim[ (~filtered_sim["ticker"].isin(blacklist_tickers)) & \
                                          (filtered_sim["date"] == date)]
                if todays_sim.index.size >= 1:
                    offerings = todays_sim.sort_values("weekly_predicted_delta",ascending=False)
                    for offering in range(offerings.index.size):
                        try:
                            trade_ticker = offerings.iloc[offering]["ticker"]
                            try:
                                trade = sim[(sim["ticker"] == trade_ticker) & (sim["date"] > date)].iloc[0]
                                sell_date = trade["date"] + timedelta(days=1)
                                sell_trades = sim[(sim["date"] >= sell_date) & (sim["ticker"] == trade["ticker"])]
                                if sell_trades.index.size < 1:
                                    if offering == offerings.index.size - 1:
                                        date = sell_date + timedelta(days=1)
                                        print(date,"no more stock vets")
                                        break
                                    else:
                                        print(date,"stock had no more listed prices")
                                        continue
                                else:
                                    if date > datetime(2020,12,14):
                                        max_hpr = int((end - date).days) - 1
                                    else:
                                        max_hpr = weekly_hpr
                                    sell_trades["gain"] = (sell_trades["adjclose"] - trade["adjclose"]) / trade["adjclose"]
                                    sell_trades["hpr"] = [(x - trade["date"]).days for x in sell_trades["date"]]
                                    if fixed:
                                        sell_trades["delta_req"] = float(weekly_delta_req)
                                    else:
                                        sell_trades["delta_req"] = [float((1+daily_delta_req) ** (math.floor(x/weekly_hpr)*weekly_hpr) - 1) for x in sell_trades["hpr"]]
                                    hpr_sell_trades = sell_trades[sell_trades["hpr"] <= max_hpr]
                                    hpr_sell_trades["hit"] = hpr_sell_trades["gain"] >= hpr_sell_trades["delta_req"]
                                    delta_hit = hpr_sell_trades[(hpr_sell_trades["hit"] == True)]
                                    if delta_hit.index.size < 1:
                                        exits = sell_trades[(sell_trades["adjclose"] >= trade["adjclose"]) & (sell_trades["hpr"]>max_hpr)]
                                        if exits.index.size < 1:
                                            yearly_trades = sell_trades[sell_trades["year"]==date.year]
                                            sell_trade = yearly_trades.iloc[yearly_trades.index.size - 1]
                                        else:
                                            sell_trade = exits.iloc[0]
                                    else:
                                        sell_trade = delta_hit.iloc[0]
                                    trade["sell_price"] = sell_trade["adjclose"]
                                    trade["delta_req"] = sell_trade["delta_req"]
                                    trade["sell_date"] = sell_trade["date"]
                                    trade["sell_delta"] = (sell_trade["adjclose"] - trade["adjclose"]) / trade["adjclose"]
                                    trade["hpr"] = sell_trade["hpr"]
                                    trade["seat"] = seat
                                    blacklist = blacklist.append([{"ticker":trade["ticker"],"start":date,"end":trade["sell_date"]}])
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

    @classmethod
    def backtest_weekly_md(self,start,end,sim,seats,trade_signal_score):
        trades = []
        blacklist = pd.DataFrame([{"ticker":"ZZZZZ","start":datetime(2016,4,1),"end":datetime(2016,4,14)}])
        for col in sim.columns:
            if 'delta' in col:
                sim["{}_abs".format(col)] = [abs(x) for x in sim[col]]
                filtered_sim = sim[(sim["{}_abs".format(col)] <= 1)]
        for col in sim.columns:
            if "regression_score" in col:
                filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
        filtered_sim = filtered_sim[(filtered_sim["weekly_predicted_delta"] > 0)]
        for seat in range(seats):
            date = start
            while date <= end:
                if date >= end:
                    break
                if date.weekday() > 4 \
                        or date == datetime(2020,2,20):
                    date = date + timedelta(days=7-date.weekday())
                blacklist_tickers = blacklist[(blacklist["start"] <= date) & (blacklist["end"] >= date)]["ticker"]
                todays_sim = filtered_sim[ (~filtered_sim["ticker"].isin(blacklist_tickers)) & \
                                          (filtered_sim["date"] == date)]
                if todays_sim.index.size >= 1:
                    offerings = todays_sim.sort_values("weekly_predicted_delta",ascending=False)
                    for offering in range(offerings.index.size):
                        try:
                            trade_ticker = offerings.iloc[offering]["ticker"]
                            try:
                                trade = offerings.iloc[offering]
                                sell_date = trade["date"] + timedelta(days=2)
                                sell_trades = sim[(sim["date"] >= sell_date) & (sim["ticker"] == trade["ticker"])]
                                if sell_trades.index.size < 1:
                                    if offering == offerings.index.size - 1:
                                        date = sell_date + timedelta(days=1)
                                        print(date,"no more stock vets")
                                        break
                                    else:
                                        print(date,"stock had no more listed prices")
                                        continue
                                else:
                                    sell_trades["hpr"] = [(x - trade["date"]).days for x in sell_trades["date"]]
                                    exits = sell_trades[(sell_trades["adjclose"] >= trade["weekly_price_categorical_regression_prediction"]) & (sell_trades["year"]==date.year)]
                                    if exits.index.size < 1:
                                        yearly_trades = sell_trades[sell_trades["year"]==date.year]
                                        sell_trade = yearly_trades.iloc[yearly_trades.index.size-1]
                                        trade["sell_price"] = sell_trade["adjclose"]
                                    else:
                                        sell_trade = exits.iloc[0]
                                        trade["sell_price"] = trade["weekly_price_categorical_regression_prediction"]
                                    trade["sell_date"] = sell_trade["date"]
                                    trade["sell_delta"] = (trade["sell_price"] - trade["adjclose"]) / trade["adjclose"]
                                    trade["hpr"] = sell_trade["hpr"]
                                    trade["seat"] = seat
                                    blacklist = blacklist.append([{"ticker":trade["ticker"],"start":date,"end":trade["sell_date"]}])
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
    
    @classmethod
    def backtest_weekly_adaptive(self,start,end,sim,seats,trade_signal_score,delta):
        trades = []
        blacklist = pd.DataFrame([{"ticker":"ZZZZZ","start":datetime(2016,4,1),"end":datetime(2016,4,14)}])
        for col in sim.columns:
            if 'delta' in col:
                sim["{}_abs".format(col)] = [abs(x) for x in sim[col]]
                filtered_sim = sim[(sim["{}_abs".format(col)] <= 1)]
        for col in sim.columns:
            if "regression_score" in col:
                filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
        filtered_sim = filtered_sim[(filtered_sim["weekly_predicted_delta"] > 0)]
        for seat in range(seats):
            date = start
            while date <= end:
                if date >= end:
                    break
                if date.weekday() > 1 \
                        or date == datetime(2020,2,20):
                    date = date + timedelta(days=7-date.weekday())
                blacklist_tickers = blacklist[(blacklist["start"] <= date) & (blacklist["end"] >= date)]["ticker"]
                todays_sim = filtered_sim[ (~filtered_sim["ticker"].isin(blacklist_tickers)) & \
                                          (filtered_sim["date"] == date)]
                if todays_sim.index.size >= 1:
                    offerings = todays_sim.sort_values("weekly_predicted_delta",ascending=False)
                    for offering in range(offerings.index.size):
                        try:
                            trade_ticker = offerings.iloc[offering]["ticker"]
                            try:
                                trade = offerings.iloc[offering]
                                sell_date = trade["date"] + timedelta(days=1)
                                day = sell_date.weekday()
                                days_till_end = 4 - day
                                max_date = trade["date"] + timedelta(days=days_till_end)
                                ask_price = trade["adjclose"] * (1+delta)
                                sell_trades = sim[(sim["date"] >= sell_date) & (sim["ticker"] == trade["ticker"]) & (sim["date"] <= max_date)]
                                if sell_trades.index.size < 1:
                                    if offering == offerings.index.size - 1:
                                        date = sell_date + timedelta(days=1)
                                        print(date,"no more stock vets")
                                        break
                                    else:
                                        print(date,"stock had no more listed prices")
                                        continue
                                else:
                                    sell_trades["hpr"] = [(x - trade["date"]).days for x in sell_trades["date"]]
                                    exits = sell_trades[(sell_trades["adjclose"] >= ask_price)]
                                    if exits.index.size < 1:
                                        sell_trade = sell_trades.iloc[sell_trades.index.size - 1]
                                    else:
                                        sell_trade = exits.iloc[0]
                                    trade["sell_price"] = sell_trade["adjclose"]
                                    trade["sell_date"] = sell_trade["date"]
                                    trade["sell_delta"] = (trade["sell_price"] - trade["adjclose"]) / trade["adjclose"]
                                    trade["hpr"] = sell_trade["hpr"]
                                    trade["seat"] = seat
                                    blacklist = blacklist.append([{"ticker":trade["ticker"],"start":date,"end":trade["sell_date"]}])
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

    @classmethod
    def backtest_weekly(self,start,end,sim,seats,extreme,ceiling,fixed,classification,value,weekly_delta_req,trade_signal_score):
        trades = []
        weekly_hpr = 7
        blacklist = pd.DataFrame([{"ticker":"ZZZZZ","start":datetime(2016,4,1),"end":datetime(2016,4,14)}])
        daily_delta_req = float(np.log(1+weekly_delta_req)/weekly_hpr)
        if not value:
            for col in sim.columns:
                if "delta" in col:
                    sim[col] = sim[col] * -1
            if extreme and classification:
                for col in sim.columns:
                    if "classification_prediction" in col:
                        sim[col] = [0 if x == 1 else 1 for x in sim[col]]
        if ceiling:
            for col in sim.columns:
                    if 'delta' in col:
                        sim["{}_abs".format(col)] = [abs(x) for x in sim[col]]
                        filtered_sim = sim[(sim["{}_abs".format(col)] <= 1)]
        else:
            filtered_sim = sim
        for col in sim.columns:
            if "regression_score" in col:
                filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
        if not fixed:
            filtered_sim = filtered_sim[(filtered_sim["weekly_predicted_delta"] >= float((1+daily_delta_req)**weekly_hpr) - 1)]
        else:
            filtered_sim = filtered_sim[(filtered_sim["weekly_predicted_delta"] >= float(weekly_delta_req))]
        if classification:
            for col in sim.columns:
                if "classification_score" in col:
                    filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
                if "classification_prediction" in col:
                    filtered_sim = filtered_sim[filtered_sim[col] == 1]
        for seat in range(seats):
            date = start
            while date <= end:
                if date >= end:
                    break
                if date.weekday() > 4 \
                        or date == datetime(2020,2,20):
                    date = date + timedelta(days=7-date.weekday())
                blacklist_tickers = blacklist[(blacklist["start"] <= date) & (blacklist["end"] >= date)]["ticker"]
                todays_sim = filtered_sim[ (~filtered_sim["ticker"].isin(blacklist_tickers)) & \
                                          (filtered_sim["date"] == date)]
                if todays_sim.index.size >= 1:
                    offerings = todays_sim.sort_values("weekly_predicted_delta",ascending=False)
                    for offering in range(offerings.index.size):
                        try:
                            trade_ticker = offerings.iloc[offering]["ticker"]
                            try:
                                trade = sim[(sim["ticker"] == trade_ticker) & (sim["date"] > date)].iloc[0]
                                sell_date = trade["date"] + timedelta(days=1)
                                sell_trades = sim[(sim["date"] >= sell_date) & (sim["ticker"] == trade["ticker"])]
                                if sell_trades.index.size < 1:
                                    if offering == offerings.index.size - 1:
                                        date = sell_date + timedelta(days=1)
                                        print(date,"no more stock vets")
                                        break
                                    else:
                                        print(date,"stock had no more listed prices")
                                        continue
                                else:
                                    if date > datetime(2020,12,14):
                                        max_hpr = int((end - date).days) - 1
                                    else:
                                        max_hpr = weekly_hpr
                                    sell_trades["gain"] = (sell_trades["adjclose"] - trade["adjclose"]) / trade["adjclose"]
                                    sell_trades["hpr"] = [(x - trade["date"]).days for x in sell_trades["date"]]
                                    if fixed:
                                        sell_trades["delta_req"] = sell_trades["weekly_predicted_delta"]
                                    else:
                                        sell_trades["delta_req"] = [float((1+daily_delta_req) ** (math.floor(x/weekly_hpr)*weekly_hpr) - 1) for x in sell_trades["hpr"]]
                                    hpr_sell_trades = sell_trades[sell_trades["hpr"] <= max_hpr]
                                    hpr_sell_trades["hit"] = hpr_sell_trades["gain"] >= hpr_sell_trades["delta_req"]
                                    delta_hit = hpr_sell_trades[(hpr_sell_trades["hit"] == True)]
                                    if delta_hit.index.size < 1:
                                        exits = sell_trades[(sell_trades["gain"] >= sell_trades["delta_req"]) & (sell_trades["hpr"]>max_hpr) & (sell_trades["year"]==date.year)]
                                        if exits.index.size < 1:
                                            yearly_trades = sell_trades[sell_trades["year"]==date.year]
                                            sell_trade = yearly_trades.iloc[yearly_trades.index.size - 1]
                                            trade["sell_price"] = trade["adjclose"] * (1+sell_trade["delta_req"])
                                        else:
                                            sell_trade = exits.iloc[0]
                                            trade["sell_price"] = trade["adjclose"]
                                    else:
                                        sell_trade = delta_hit.iloc[0]
                                        trade["sell_price"] = trade["adjclose"] * (1+sell_trade["delta_req"])
                                    trade["delta_req"] = sell_trade["delta_req"]
                                    trade["sell_date"] = sell_trade["date"]
                                    trade["sell_delta"] = (trade["sell_price"] - trade["adjclose"]) / trade["adjclose"]
                                    trade["hpr"] = sell_trade["hpr"]
                                    trade["seat"] = seat
                                    blacklist = blacklist.append([{"ticker":trade["ticker"],"start":date,"end":trade["sell_date"]}])
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

    @classmethod
    def backtest_quarterly(self,start,end,sim,seats,extreme,ceiling,fixed,classification,value,quarterly_delta_req,trade_signal_score):
        trades = []
        quarterly_hpr = 90
        blacklist = pd.DataFrame([{"ticker":"ZZZZZ","start":datetime(2016,4,1),"end":datetime(2016,4,14)}])
        daily_delta_req = float(np.log(1+quarterly_delta_req)/quarterly_hpr)
        if not value:
            for col in sim.columns:
                if "delta" in col:
                    sim[col] = sim[col] * -1
            if extreme and classification:
                for col in sim.columns:
                    if "classification_prediction" in col:
                        sim[col] = [0 if x == 1 else 1 for x in sim[col]]
        if ceiling:
            for col in sim.columns:
                    if 'delta' in col:
                        sim["{}_abs".format(col)] = [abs(x) for x in sim[col]]
                        filtered_sim = sim[(sim["{}_abs".format(col)] <= 1)]
        else:
            filtered_sim = sim
        for col in sim.columns:
            if "regression_score" in col:
                filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
        if not fixed:
            filtered_sim = filtered_sim[(filtered_sim["quarterly_predicted_delta"] >= float((1+daily_delta_req)**quarterly_hpr) - 1)]
        else:
            filtered_sim = filtered_sim[(filtered_sim["quarterly_predicted_delta"] >= float(quarterly_delta_req))]
        if classification:
            for col in sim.columns:
                if "classification_score" in col:
                    filtered_sim = filtered_sim[filtered_sim[col] >= trade_signal_score]
                if "classification_prediction" in col:
                    filtered_sim = filtered_sim[filtered_sim[col] == 1]
        for seat in range(seats):
            date = start
            while date <= end:
                if date >= end:
                    break
                if date.weekday() > 4 \
                        or date == datetime(2020,2,20):
                    date = date + timedelta(days=7-date.weekday())
                blacklist_tickers = blacklist[(blacklist["start"] <= date) & (blacklist["end"] >= date)]["ticker"]
                todays_sim = filtered_sim[ (~filtered_sim["ticker"].isin(blacklist_tickers)) & \
                                          (filtered_sim["date"] == date)]
                if todays_sim.index.size >= 1:
                    offerings = todays_sim.sort_values("quarterly_predicted_delta",ascending=False)
                    for offering in range(offerings.index.size):
                        try:
                            trade_ticker = offerings.iloc[offering]["ticker"]
                            try:
                                trade = sim[(sim["ticker"] == trade_ticker) & (sim["date"] > date)].iloc[0]
                                sell_date = trade["date"] + timedelta(days=1)
                                sell_trades = sim[(sim["date"] >= sell_date) & (sim["ticker"] == trade["ticker"])]
                                if sell_trades.index.size < 1:
                                    if offering == offerings.index.size - 1:
                                        date = sell_date + timedelta(days=1)
                                        print(date,"no more stock vets")
                                        break
                                    else:
                                        print(date,"stock had no more listed prices")
                                        continue
                                else:
                                    if date > datetime(2020,12,14):
                                        max_hpr = int((end - date).days) - 1
                                    else:
                                        max_hpr = quarterly_hpr
                                    sell_trades["gain"] = (sell_trades["adjclose"] - trade["adjclose"]) / trade["adjclose"]
                                    sell_trades["hpr"] = [(x - trade["date"]).days for x in sell_trades["date"]]
                                    if fixed:
                                        sell_trades["delta_req"] = float(quarterly_delta_req)
                                    else:
                                        sell_trades["delta_req"] = [float((1+daily_delta_req) ** (math.floor(x/quarterly_hpr)*quarterly_hpr) - 1) for x in sell_trades["hpr"]]
                                    hpr_sell_trades = sell_trades[sell_trades["hpr"] <= max_hpr]
                                    hpr_sell_trades["hit"] = hpr_sell_trades["gain"] >= hpr_sell_trades["delta_req"]
                                    delta_hit = hpr_sell_trades[(hpr_sell_trades["hit"] == True)]
                                    if delta_hit.index.size < 1:
                                        exits = sell_trades[(sell_trades["adjclose"] >= trade["adjclose"]) & (sell_trades["hpr"]>max_hpr)]
                                        if exits.index.size < 1:
                                            yearly_trades = sell_trades[sell_trades["year"]==date.year]
                                            sell_trade = yearly_trades.iloc[yearly_trades.index.size - 1]
                                        else:
                                            sell_trade = exits.iloc[0]
                                    else:
                                        sell_trade = delta_hit.iloc[0]
                                    trade["sell_price"] = sell_trade["adjclose"]
                                    trade["delta_req"] = sell_trade["delta_req"]
                                    trade["sell_date"] = sell_trade["date"]
                                    trade["sell_delta"] = (sell_trade["adjclose"] - trade["adjclose"]) / trade["adjclose"]
                                    trade["hpr"] = sell_trade["hpr"]
                                    trade["seat"] = seat
                                    blacklist = blacklist.append([{"ticker":trade["ticker"],"start":date,"end":trade["sell_date"]}])
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