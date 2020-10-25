import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np

class PreModelTransformer(object):    
    def __init__(self,ticker):
        self.ticker = ticker
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
                                    ,"_id"
                                    ,"id_x","id_y"
                                    ,"filed"
                                    ,"splitfactor"
                                    ]
    
    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def fundamental_daily_merge(self,price,funds):
        price.reset_index(inplace=True)
        relev = price[["Date","Adj_Close"]]
        for column in ["Date","Adj_Close"]:
            relev.rename(columns={column:"".join(column.split("_")).lower()},inplace=True)
        relev["date"] = [datetime.strptime(x.split("T")[0],"%Y-%m-%d") for x in relev["date"]]
        relev.rename(columns={"adjClose":self.ticker},inplace=True)
        relev["year"] = [x.year for x in relev["date"]]
        relev["quarter"] = [x.quarter for x in relev["date"]]
        relev["month"] = [x.month for x in relev["date"]]
        relev["dayOfWeek"] = [x.weekday() for x in relev["date"]]
        relev["dayOfQuarter"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                            ,3 * row[1]["quarter"] -2,1)).days
                                for row in relev.iterrows()]
        relev["dayOfMonth"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                            ,row[1]["date"].month,1)).days
                                for row in relev.iterrows()]
        relev["dayOfYear"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                ,1,1)).days
                    for row in relev.iterrows()]
        total_days = {"Year":365,"Month":31,"Week":6,"Quarter":90}
        for timerange in ["Year","Quarter","Month","Week"]:
            col_name = "dayOf{}".format(timerange)
            relev["dayOf{}Sine".format(timerange)] = [np.sin((x) * (2*np.pi/total_days[timerange])) for x in relev[col_name]]
            relev["dayOf{}Cosine".format(timerange)] = [np.cos((x) * (2*np.pi/total_days[timerange])) for x in relev[col_name]]
        relev.reset_index(inplace=True)
        for column in relev.columns:
            relev.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
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
            relev["date"] = pd.to_datetime(relev["date"],utc=True)
        except:
            relev["date"] = relev["date"]
        relev["quarter"] = [x.quarter - 1 if x.quarter > 1 else 4 for x in relev["date"]]
        relev["year"] = [row[1]["date"].year if row[1]["quarter"] < 4 else row[1]["date"].year - 1 for row in relev.iterrows()]
        final = relev.merge(funds,on=["year","quarter"],how="left")
        final = final[(final["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                    & (final["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                    & (final["adjclose"] > 0)]
        cleaned = final.drop(self.drop_columns,axis=1,errors="ignore")
        cleaned.drop_duplicates(inplace=True)
        cleaned = cleaned.groupby("date").mean()
        cleaned["ticker"] = self.ticker
        cleaned.reset_index(inplace=True)
        cleaned.sort_values("date",inplace=True)
        return cleaned
    
    def daily_merge(self,price,shift):
        price.reset_index(inplace=True)
        relev = price
        relev["year"] = [x.year for x in relev["date"]]
        relev["quarter"] = [x.quarter for x in relev["date"]]
        relev["month"] = [x.month for x in relev["date"]]
        relev["dayOfWeek"] = [x.weekday() for x in relev["date"]]
        relev["dayOfQuarter"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                            ,3 * row[1]["quarter"] -2,1)).days
                                for row in relev.iterrows()]
        relev["dayOfMonth"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                            ,row[1]["date"].month,1)).days
                                for row in relev.iterrows()]
        relev["dayOfYear"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                ,1,1)).days
                    for row in relev.iterrows()]
        total_days = {"Year":365,"Month":31,"Week":6,"Quarter":90}
        for timerange in ["Year","Quarter","Month","Week"]:
            col_name = "dayOf{}".format(timerange)
            relev["dayOf{}Sine".format(timerange)] = [np.sin((x) * (2*np.pi/total_days[timerange])) for x in relev[col_name]]
            relev["dayOf{}Cosine".format(timerange)] = [np.cos((x) * (2*np.pi/total_days[timerange])) for x in relev[col_name]]
        relev["y"] = relev[self.ticker].shift(-shift)
        relev["label_date"]= relev["date"].shift(-shift)
        relev = relev[:-shift]
        return relev
    
    def daily_prediction_merge(self,price):
        price.reset_index(inplace=True)
        relev = price
        relev["year"] = [x.year for x in relev["date"]]
        relev["quarter"] = [x.quarter for x in relev["date"]]
        relev["month"] = [x.month for x in relev["date"]]
        relev["dayOfWeek"] = [x.weekday() for x in relev["date"]]
        relev["dayOfQuarter"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                            ,3 * row[1]["quarter"] -2,1)).days
                                for row in relev.iterrows()]
        relev["dayOfMonth"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                            ,row[1]["date"].month,1)).days
                                for row in relev.iterrows()]
        relev["dayOfYear"] = [(row[1]["date"] - datetime(row[1]["date"].year
                                                ,1,1)).days
                    for row in relev.iterrows()]
        total_days = {"Year":365,"Month":31,"Week":6,"Quarter":90}
        for timerange in ["Year","Quarter","Month","Week"]:
            col_name = "dayOf{}".format(timerange)
            relev["dayOf{}Sine".format(timerange)] = [np.sin((x) * (2*np.pi/total_days[timerange])) for x in relev[col_name]]
            relev["dayOf{}Cosine".format(timerange)] = [np.cos((x) * (2*np.pi/total_days[timerange])) for x in relev[col_name]]
        return relev


