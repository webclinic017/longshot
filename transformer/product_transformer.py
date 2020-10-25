import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np
class ProductTransformer(object):    
    def __init__(self,ticker,start,end):
        self.ticker = ticker
        self.start = start
        self.end = end
        ## might need to drop years from columns here
        self.drop_columns = [
                            # 'Date',
                            'Open',
                            'High',
                            'Low',
                            'Close',
                            'Volume',
                            'Dividend',
                            'Split',
                            'Adj_Open',
                            'Adj_High',
                            'Adj_Low',
                            "Split",
                            "Dividend",
                            # 'Adj_Close',
                            'Adj_Volume',
                            "filed","_id","cik","ticker","ticker_x","ticker_y","quarter","year","index","level_0","_id_x","_id_y","adsh","score"]

    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_unified(self,price,factors,price_factors,fundamental_results,price_results,previous_average):
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        for i in range(len(fundamental_results)):
            fundamental_result = fundamental_results.iloc[i]
            fundamental_model = fundamental_result["model"]
            prediction = fundamental_model.predict(factors[i]["X"])
            if fundamental_result["classification"]:
                col_name = "classification"
            else:
                col_name = "regression"
            price["fundamental_{}_prediction".format(col_name)] = prediction[0]
            price["fundamental_{}_score".format(col_name)] = fundamental_result["score"]
        for i in range(len(price_results)):
            price_result = price_results.iloc[i]
            price_model = price_result["model"]
            prediction = price_model.predict(price_factors[i]["X"])
            if price_result["classification"]:
                col_name = "classification"
            else:
                col_name = "regression"
            price["price_{}_prediction".format(col_name)] = prediction[0]
            price["price_{}_score".format(col_name)] = fundamental_result["score"]
        price.reset_index(inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        price.rename(columns={"dailyregressionscore":"daily_regression_score"
                            ,"dailyregressionprediction":"daily_regression_prediction"
                            ,"dailyclassificationprediction":"daily_classification_prediction"
                            ,"dailyclassificationscore":"daily_classification_score"},inplace=True)
        cleaned = price
        cleaned["ticker"] = self.ticker
        cleaned["pqa"] = previous_average
        qmtd = []
        for i in range(len(cleaned)):
            qmtd.append(cleaned.iloc[:i]["adjclose"].max())
        cleaned["qmtd"] = qmtd
        cleaned["qd"] = cleaned["qmtd"] - cleaned["adjclose"]
        cleaned["passed"] = [1 if x > 0 else 0 for x in cleaned["qd"]]
        cleaned = cleaned[(cleaned["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (cleaned["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
        cols = ["date","ticker","pqa"
                        ,"adjclose","qmtd","passed"]
        cols.extend([x for x in cleaned.columns if "regression" in x or "classification" in x])
        return cleaned[cols]
    
    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge(self,price,factors,price_factors,fundamental_results,price_results):
        fundamental_model = fundamental_results["model"]
        price_model = price_results["model"]
        prediction = fundamental_model.predict(factors)
        price_prediction =price_model.predict(price_factors)
        price.reset_index(inplace=True)
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        price.rename(columns={"dailyaccuracy":"daily_accuracy","daily_prediction":"daily_prediction"},inplace=True)
        price["fundamental_prediction"] = prediction[0]
        price["price_prediction"] = price_prediction[0]
        cleaned = price
        cleaned["ticker"] = self.ticker
        cleaned["fundamental_accuracy"] = fundamental_results["accuracy"]
        cleaned["price_accuracy"] = price_results["accuracy"]
        cleaned["fundamental_delta"] = (cleaned["fundamental_prediction"] - cleaned["adjclose"]) / cleaned["adjclose"]
        cleaned["price_delta"] = (cleaned["price_prediction"] - cleaned["adjclose"]) / cleaned["adjclose"]
        cleaned = cleaned[(cleaned["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (cleaned["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
        return cleaned[["date","ticker"
                        ,"adjclose","fundamental_prediction"
                        ,"fundamental_delta","fundamental_accuracy"
                        ,"price_prediction","price_delta","price_accuracy",
                        "daily_prediction","daily_accuracy"]]
    
    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_quarterlies_price(self,price,price_factors,price_results):
        price.reset_index(inplace=True)
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        price.sort_values(by="date",ascending=True,inplace=True)
        price = price[(price["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (price["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
        for i in range(len(price_results)):
            price_result = price_results.iloc[i]
            price_model = price_result["model"]
            prediction = price_model.predict(price_factors[i]["X"])
            if price_result["classification"]:
                col_name = "classification"
            else:
                col_name = "regression"
            price["quarterly_price_{}_prediction".format(col_name)] = prediction[0]
            price["quarterly_price_{}_score".format(col_name)] = price_result["score"]
        price.reset_index(inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        cleaned = price
        cleaned["ticker"] = self.ticker
        qmtd = []
        for i in range(len(cleaned)):
            current_date = cleaned.iloc[i]["date"] 
            window = cleaned.iloc[0:i+1]
            max_val = window["adjclose"].max()
            qmtd.append(max_val)
        cleaned["qmtd"] = qmtd
        cleaned["qd"] = cleaned["qmtd"] - cleaned["adjclose"]
        cleaned["passed"] = [1 if x > 0 else 0 for x in cleaned["qd"]]
        cleaned = cleaned[(cleaned["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (cleaned["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
        cols = ["date","ticker","adjclose","qmtd","passed"]
        cols.extend([x for x in cleaned.columns if "regression" in x or "classification" in x])
        return cleaned[cols]

    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_quarterlies_fundamental(self,price,factors,fundamental_results):
        price.reset_index(inplace=True)
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        price.sort_values(by="date",ascending=True,inplace=True)
        price = price[(price["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (price["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
        for i in range(len(fundamental_results)):
            fundamental_result = fundamental_results.iloc[i]
            fundamental_model = fundamental_result["model"]
            prediction = fundamental_model.predict(factors[i]["X"])
            if fundamental_result["classification"]:
                col_name = "classification"
            else:
                col_name = "regression"
            price["quarterly_fundamental_{}_prediction".format(col_name)] = prediction[0]
            price["quarterly_fundamental_{}_score".format(col_name)] = fundamental_result["score"]
        price.reset_index(inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        cleaned = price
        cleaned["ticker"] = self.ticker
        qmtd = []
        for i in range(len(cleaned)):
            current_date = cleaned.iloc[i]["date"] 
            window = cleaned.iloc[0:i+1]
            max_val = window["adjclose"].max()
            qmtd.append(max_val)
        cleaned = cleaned[(cleaned["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (cleaned["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
        cols = ["date","ticker","adjclose"]
        cols.extend([x for x in cleaned.columns if "regression" in x or "classification" in x])
        return cleaned[cols]
    
    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_weeklies(self,price,price_factors,price_results):
        price.reset_index(inplace=True)
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        price.sort_values(by="date",ascending=True,inplace=True)
        price = price[(price["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (price["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
        for i in range(len(price_results)):
            price_result = price_results.iloc[i]
            price_model = price_result["model"]
            prediction = price_model.predict(price_factors[i]["X"])
            if price_result["classification"]:
                col_name = "classification"
            else:
                col_name = "regression"
            price["weekly_price_{}_prediction".format(col_name)] = prediction[0]
            price["weekly_price_{}_score".format(col_name)] = price_result["score"]
        cleaned = price
        cleaned["ticker"] = self.ticker
        wmtd = []
        for i in range(len(cleaned)):
            current_date = cleaned.iloc[i]["date"] 
            window = cleaned.iloc[0:i+1]
            max_val = window["adjclose"].max()
            wmtd.append(max_val)
        cleaned["wmtd"] = wmtd
        # cleaned["wmtdd"] = wmtdd
        cleaned["wd"] = cleaned["wmtd"] - cleaned["weekly_price_regression_prediction"]
        cleaned["passed"] = [1 if x > 0 else 0 for x in cleaned["wd"]]
        cols = ["date","ticker","adjclose","wmtd"]
        cols.extend([x for x in cleaned.columns if "regression" in x or "classification" in x])
        return cleaned[cols]

    # ##TODO reaggregate based on filed or some other date 
    # ##TODO double check indexing
    # def merge_tf(self,price,factors,price_factors,fundamental_results,price_results):
    #     fundamental_model = fundamental_results["model"]
    #     price_model = price_results["model"]
    #     prediction = fundamental_model.predict(factors)
    #     price_prediction =price_model.predict(price_factors)
    #     price.reset_index(inplace=True)
    #     for column in price.columns:
    #         price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
    #     try:
    #         price["date"] = pd.to_datetime(price["date"],utc=True)
    #     except:
    #         price["date"] = price["date"]
    #     price.rename(columns={"dailyaccuracy":"daily_accuracy","daily_prediction":"daily_prediction"},inplace=True)
    #     price["fundamental_prediction"] = prediction[0][0]
    #     price["price_prediction"] = price_prediction[0][0]
    #     cleaned = price
    #     cleaned["ticker"] = self.ticker
    #     cleaned["fundamental_accuracy"] = fundamental_results["accuracy"]
    #     cleaned["price_accuracy"] = price_results["accuracy"]
    #     cleaned["fundamental_delta"] = (cleaned["fundamental_prediction"] - cleaned["adjclose"]) / cleaned["adjclose"]
    #     cleaned["price_delta"] = (cleaned["price_prediction"] - cleaned["adjclose"]) / cleaned["adjclose"]
    #     cleaned = cleaned[(cleaned["date"] >= datetime.strptime(self.start,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
    #                         & (cleaned["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc))]
    #     return cleaned[["date","ticker"
    #                     ,"adjclose","fundamental_prediction"
    #                     ,"fundamental_delta","fundamental_accuracy"
    #                     ,"price_prediction","price_delta","price_accuracy",
    #                     "daily_prediction","daily_accuracy"]]

    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_day_trade(self,ticker,factors,model):
        prediction = model.predict(factors)
        print(prediction)
        factors["predicted"] = prediction.iloc[0].item()
        cleaned = factors.drop(self.drop_columns,axis=1,errors="ignore")
        cleaned["ticker"] = self.ticker
        cleaned["delta"] = (cleaned["predicted"] - cleaned[ticker]) / cleaned[ticker]
        cleaned.rename(columns={ticker:"Adj_Close"})
        return cleaned[["Date","Adj_Close","predicted","delta","ticker"]]