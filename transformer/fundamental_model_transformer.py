import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np

class ModelTransformer(object):    
    def __init__(self,ticker,start,end,yearly_gap):
        self.ticker = ticker
        self.yearly_gap = yearly_gap
        self.quarter = quarter
        self.year = year
        ## might need to drop years from columns here
        self.drop_columns = ["filed","cik","adsh"]

    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def fundamental_merge_rec(self,funds,quarterly,factors):
        funds.reset_index(inplace=True)
        fund_drop_columns = []
        ## omit fundamental column renaming
        for column in funds.columns:
            if len([x for x in funds.columns if x == column]) > 1:
                fund_drop_columns.append(column)
        funds.drop(fund_drop_columns,axis=1,inplace=True)
        ## date transformations and gap management
        funds["date"] = [datetime.strptime(str(x),"%Y%m%d").replace(tzinfo=timezone.utc) if "-" not in str(x) else \
                            datetime.strptime(str(x).split(" ")[0],"%Y-%m-%d").replace(tzinfo=timezone.utc) for x in funds["filed"]]
        funds["quarter"] = [x.quarter for x in funds["date"]]
        funds["year"] = [x.year for x in funds["date"]]
        funds = funds.groupby(["year","quarter"]).mean().reset_index()
        ## merge
        final = funds
        final = final[(final["year"] == year) & (final["quarter"] == quarter)]
        cleaned = final.drop(self.drop_columns,axis=1,errors="ignore")
        cleaned.drop_duplicates(inplace=True)
        for column in cleaned.columns:
            if column not in factors:
                cleaned.drop(column,axis=1,inplace=True)
        ## adding ticker column
        cleaned["ticker"] = self.ticker
        return cleaned
    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing

    def fundamental_merge(self,year,quarter,training_years,gap,price,funds,classify):
        funds.reset_index(inplace=True)
        price.reset_index(inplace=True)
        fund_drop_columns = []
        ## omit fundamental column renaming
        for col in funds.columns:
            if funds[col].isnull().values.any():
                funds.drop(col,axis=1,inplace=True)
        ## date transformations and gap management
        funds["filed"] = [datetime.strptime(str(x),"%Y%m%d").replace(tzinfo=timezone.utc) if "-" not in str(x) else \
                            datetime.strptime(str(x).split(" ")[0],"%Y-%m-%d").replace(tzinfo=timezone.utc) for x in funds["filed"]]
        funds["quarter"] = [x.quarter for x in funds["filed"]]
        funds["year"] = [x.year for x in funds["filed"]]
        ## merge
        final = price[].merge(funds,on=["year","quarter"],how="left")

        ## date filtering
        first = final[(final["year"] == year - training_years) & (final["quarter"] == quarter)].index.values.tolist()[0]
        last = final[(final["year"] == year) & (final["quarter"] == quarter)].index.values.tolist()[0]
        final = final.iloc[first:last-1]

        cleaned = final.drop(self.drop_columns,axis=1,errors="ignore")

        ## classification y column
        if classify:
            cleaned["adjclose"] = cleaned["adjclose"].diff()
            cleaned["adjclose"] = [1 if x > 0 else 0 for x in cleaned["adjclose"]]
            cleaned.reset_index(inplace=True)
        cleaned.drop_duplicates(inplace=True)

        ## adding ticker column
        cleaned["ticker"] = self.ticker
        return cleaned