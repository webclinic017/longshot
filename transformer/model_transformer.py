import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np

class ModelTransformer(object):    
    def __init__(self,ticker,start,end,reporting_gap):
        self.ticker = ticker
        self.reporting_gap = reporting_gap
        self.start = start
        self.end = end
        ## might need to drop years from columns here
        self.drop_columns = [
                                    'open',
                                    'high',
                                    'low',
                                    'close',
                                    'volume',
                                    'dividend',
                                    'split',
                                    'adjopen',
                                    'adjhigh',
                                    'adjlow',
                                    "split",
                                    "dividend",
                                    'adjvolume',
                                    "divcash",
                                    "splitfactor"
                                    "filed","_id","cik"
                                    ,"ticker","ticker_x"
                                    ,"ticker_y"
                                    ,"date"
                                    ,"year"
                                    ,"index","level_0"
                                    ,"_id_x","_id_y","adsh","score",
                                    "level_0"
                                    ,"index_x"
                                    ,"index_y"
                                    ,"reporting_gap"
                                    ,"_id"
                                    ,"id_x","id_y"
                                    ,"filed"
                                    ,"splitfactor"
                                    ]


    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def fundamental_merge(self,price,funds,quarterly,classify):
        funds.reset_index(inplace=True)
        price.reset_index(inplace=True)
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        fund_drop_columns = []
        for column in funds.columns:
            if len([x for x in funds.columns if x == column]) > 1:
                fund_drop_columns.append(column)
        funds.drop(fund_drop_columns,axis=1,inplace=True)
        funds["filed"] = [datetime.strptime(str(x),"%Y%m%d").replace(tzinfo=timezone.utc) if "-" not in str(x) else \
                            datetime.strptime(str(x).split(" ")[0],"%Y-%m-%d").replace(tzinfo=timezone.utc) for x in funds["filed"]]
        funds["quarter"] = [x.quarter for x in funds["filed"]]
        funds["year"] = [x.year for x in funds["filed"]]
        price["date"] = pd.to_datetime(price["date"],utc=True)
        price["quarter"] = [(x - timedelta(days=self.reporting_gap)).quarter for x in price["date"]]
        price["year"] = [(x - timedelta(days=self.reporting_gap)).year for x in price["date"]]
        final = price.merge(funds,on=["year","quarter"],how="left")
        final = final[
                    (final["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                    & (final["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                    & (final["adjclose"] > 0)]
        if quarterly:
            final = final.groupby(by=["year","quarter"]).mean()
        cleaned = final.drop(self.drop_columns,axis=1,errors="ignore")
        if classify:
            cleaned["adjclose"] = cleaned["adjclose"].diff()
            cleaned["adjclose"] = [1 if x > 0 else 0 for x in cleaned["adjclose"]]
            cleaned.reset_index(inplace=True)
        cleaned.drop_duplicates(inplace=True)
        cleaned["ticker"] = self.ticker
        return cleaned
    
    def quarterly_price_transform(self,model_data,ticker,start_year,start_quarter,end_year,end_quarter,gap):
        first_index = model_data[(model_data["year"] == start_year) & (model_data["quarter"] == start_quarter)].index.values.tolist()[0]
        last_index = model_data[(model_data["year"] == end_year) & (model_data["quarter"] == end_quarter)].index.values.tolist()[-1]
        ticker_data = model_data.iloc[first_index:last_index+1]
        ticker_data["y"] = ticker_data[ticker]
        ticker_data["y"] = ticker_data["y"].shift(-gap)
        ticker_data = ticker_data[:-gap]
        return ticker_data
    
    def weekly_price_transform(self,model_data,ticker,start_year,start_week,end_year,end_week,gap):
        first_index = model_data[(model_data["year"] == start_year) & (model_data["week"] == start_week)].index.values.tolist()[0]
        last_index = model_data[(model_data["year"] == end_year) & (model_data["week"] == end_week)].index.values.tolist()[-1]
        ticker_data = model_data.iloc[first_index:last_index+1]
        ticker_data["y"] = ticker_data[ticker]
        ticker_data["y"] = ticker_data["y"].shift(-gap)
        ticker_data = ticker_data[:-gap]
        return ticker_data
    
    def daily_price_transform(self,ticker_data,ticker,gap):
        ticker_data["y"] = ticker_data[ticker]
        ticker_data["y"] = ticker_data["y"].shift(-gap)
        ticker_data = ticker_data[:-gap]
        return ticker_data
    
    def trade_signal_transform_classify(self,data):
        labels = []
        for i in range(len(data)):
            row = data.iloc[i].copy()
            buy_price = row["adjclose"]
            offerings = data.iloc[i:i+5]
            offerings["delta"] = (offerings["adjclose"] - buy_price) / buy_price
            if offerings[offerings["delta"] > 0].index.size > 0:
                labels.append(1)
            else:
                labels.append(0)
        data["y"] = labels
        return data
    
    def trade_signal_transform_regression(self,data):
        labels = []
        for i in range(len(data)):
            row = data.iloc[i].copy()
            buy_price = row["adjclose"]
            offerings = data.iloc[i:i+5]
            offerings["delta"] = (offerings["adjclose"] - buy_price) / buy_price
            if offerings[offerings["delta"] > 0].index.size > 0:
                labels.append(offerings[offerings["delta"] > 0].iloc[0]["delta"])
            else:
                labels.append(offerings.tail(1)["delta"].item())
        data["y"] = labels
        return data
