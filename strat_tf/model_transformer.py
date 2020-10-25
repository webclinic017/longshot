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
    def fundamental_daily_merge(self,price,funds):
        funds.reset_index(inplace=True)
        price.reset_index(inplace=True)
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        fund_drop_columns = []
        for column in funds.columns:
            if len([x for x in funds.columns if x == column]) > 1:
                fund_drop_columns.append(column)
        funds.drop(fund_drop_columns,axis=1,inplace=True)
        try:
            funds["filed"] = pd.to_datetime(funds["filed"],format="%Y%m%d",utc=True)
        except:
            funds["filed"] = funds["filed"]
        funds["quarter"] = [x.quarter for x in funds["filed"]]
        funds["year"] = [x.year for x in funds["filed"]]
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        price["quarter"] = [(x - timedelta(days=self.reporting_gap)).quarter for x in price["date"]]
        price["year"] = [(x - timedelta(days=self.reporting_gap)).year for x in price["date"]]
        final = price.merge(funds,on=["year","quarter"],how="left")
        final["reporting_gap"] = final["date"] - final["filed"]
        final["reporting_gap"] = [x.days for x in final["reporting_gap"]]
        final = final[(final["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                    & (final["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                    & (final["reporting_gap"] <= self.reporting_gap)
                    & (final["adjclose"] > 0)]
        cleaned = final.drop(self.drop_columns,axis=1,errors="ignore")
        cleaned.drop_duplicates(inplace=True)
        cleaned["ticker"] = self.ticker
        return cleaned


