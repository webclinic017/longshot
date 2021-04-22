import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np
import pickle
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
        end = datetime.strptime(self.end,"%Y-%m-%d")
        if end.month == 12:
            end_date = datetime(end.year + 1, 3,31)
        else:
            end_month = end.month + 3
            if end_month > 12:
                end_date = datetime(end.year,12,31)
            else:
                end_date = datetime(end.year,end_month,1) - timedelta(days=1)
        price = price[(price["date"] > end.replace(tzinfo=timezone.utc)) 
                            & (price["date"] < end_date.replace(tzinfo=timezone.utc))]
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
        cols = ["date","ticker","adjclose","qmtd","passed"]
        cols.extend([x for x in cleaned.columns if "regression" in x or "classification" in x])
        return cleaned[cols]

    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_quarterlies(self,factors,fundamental_results,year,quarter,suffix,gap):
        price = {"year":year + gap,"quarter":quarter}
        for i in range(len(fundamental_results)):
            try:
                fundamental_result = fundamental_results.iloc[i]
                fundamental_model = fundamental_result["model"]
                prediction = fundamental_model.predict(factors[i]["X"])
                if fundamental_result["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]
                    prediction = p
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = prediction[len(prediction)-1]
                price["quarterly_{}_{}_score".format(suffix,col_name)] = fundamental_result["score"]
            except Exception as e:
                print(str(e))
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = -99999
                price["quarterly_{}_{}_score".format(suffix,col_name)] = -99999
        price["ticker"] = self.ticker
        return price
    
    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_quarterlies_rec(self,factors,fundamental_results,suffix,year_gap):
        start_date = datetime.strptime(self.start,"%Y-%m-%d") 
        start_date = datetime(start_date.year + year_gap, start_date.month,start_date.day)
        price = {"year":start_date.year,"quarter":((start_date.month -1)//3 + 1)}
        for i in range(len(fundamental_results)):
            try:
                fundamental_result = fundamental_results.iloc[i]
                fundamental_model = fundamental_result["model"]
                prediction = fundamental_model.predict(factors[i]["X"])
                if fundamental_result["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]
                    prediction = p
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = prediction[0]
                price["quarterly_{}_{}_score".format(suffix,col_name)] = fundamental_result["score"]
            except:
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = -99999
                price["quarterly_{}_{}_score".format(suffix,col_name)] = -99999
        price["ticker"] = self.ticker
        return price   

    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_quarterlies_price_rec(self,factors,fundamental_results,suffix,year,quarter): 
        price = {"year":year,"quarter":quarter}
        for i in range(len(fundamental_results)):
            try:
                fundamental_result = fundamental_results.iloc[i]
                fundamental_model = fundamental_result["model"]
                prediction = fundamental_model.predict(factors[i]["X"])
                if fundamental_result["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]
                    prediction = p
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = prediction[0]
                price["quarterly_{}_{}_score".format(suffix,col_name)] = fundamental_result["score"]
            except:
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = -99999
                price["quarterly_{}_{}_score".format(suffix,col_name)] = -99999
        price["ticker"] = self.ticker
        return price     

    ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_quarterlies_rank(self,factors,fundamental_results,suffix):
        start_date = datetime.strptime(self.start,"%Y-%m-%d") 
        start_date = datetime(start_date.year, start_date.month,start_date.day)
        price = {"year":start_date.year,"quarter":((start_date.month -1)//3 + 1)}
        for i in range(len(fundamental_results)):
            try:
                fundamental_result = fundamental_results.iloc[i]
                fundamental_model = fundamental_result["model"]
                prediction = fundamental_model.predict(factors[i]["X"])
                if fundamental_result["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]
                    prediction = p
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = prediction[0]
                price["quarterly_{}_{}_score".format(suffix,col_name)] = fundamental_result["score"]
            except:
                col_name = fundamental_result["model_type"]
                price["quarterly_{}_{}_prediction".format(suffix,col_name)] = -99999
                price["quarterly_{}_{}_score".format(suffix,col_name)] = -99999
        price["ticker"] = self.ticker
        return price
    
        ##TODO reaggregate based on filed or some other date 
    ##TODO double check indexing
    def merge_weeklies_predict(self,price_factors,price_results):
        predicts = {}
        for i in range(len(price_results)):
            price_result = price_results.iloc[i]
            price_model = price_result["model"]
            prediction = price_model.predict(price_factors[i]["X"])
            if price_result["classification"]:
                col_name = "classification"
            else:
                col_name = "regression"
            predicts["weekly_price_{}_prediction".format(col_name)] = prediction[0]
            predicts["weekly_price_{}_score".format(col_name)] = price_result["score"]
        predicts["ticker"] = self.ticker
        return predicts

    def merge_weeklies_v2(self,factors,price_results,product_year,product_quarter,suffix):
        current_sets = []
        number_of_models = price_results.index.size
        for i in range(number_of_models):
            try:
                price_dict = []
                for week in factors[i]["X"]["week"]:
                    price_dict.append({"year":product_year,"quarter":product_quarter,"week":week})
                price = pd.DataFrame(price_dict)
                price_result = price_results.iloc[i]
                price_model = price_result["model"]
                prediction = price_model.predict(factors[i]["X"])
                current_set = price.copy()
                if price_result["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]   
                    prediction = p
                col_name = price_result["model_type"]
                current_set["weekly_{}_{}_prediction".format(suffix,col_name)] = prediction
                current_set["weekly_{}_{}_score".format(suffix,col_name)] = price_result["score"]
                current_sets.append(current_set)
            except Exception as e:
                col_name = price_result["model_type"]
                print(self.ticker,str(e),print(col_name))
                current_set = price.copy()
                current_set["weekly_{}_{}_prediction".format(suffix,col_name)] = -99999
                current_set["weekly_{}_{}_score".format(suffix,col_name)] = -99999
                current_sets.append(current_set)
                continue
        base = current_sets[0]
        for cs in current_sets[1:]:
            base = base.merge(cs,on=["year","quarter","week"],how="left")
        base["ticker"] = self.ticker
        return base

    def merge_weeklies_calcs(self,factors,price_results,product_year,product_week,suffix,features):
        current_set = {}
        number_of_models = price_results.index.size
        for i in range(number_of_models):
            try:
                price_result = price_results.iloc[i]
                col_name = price_result["model_type"]
                current_features = features[features["model"]==col_name]["feature"].to_list()
                current_factors = factors[i]["X"]
                for feature in current_features:
                    if feature not in current_factors.columns:
                        current_factors[feature] = -99999
                price_model = pickle.loads(price_result["model"])   
                prediction = price_model.predict(current_factors[current_features])
                if price_result["api"] == "tf":
                    p = []
                    [p.extend(x) for x in prediction]   
                    prediction = p
                current_set["weekly_{}_{}_prediction".format(suffix,col_name)] = prediction[len(prediction)-1]
                current_set["weekly_{}_{}_score".format(suffix,col_name)] = price_result["score"]
            except Exception as e:
                print("failed calculation")
                col_name = price_result["model_type"]
                current_set["weekly_{}_{}_prediction".format(suffix,col_name)] = -99999
                current_set["weekly_{}_{}_score".format(suffix,col_name)] = -99999
                continue
        current_set["week"] = product_week
        current_set["year"] = product_year
        current_set["ticker"] = self.ticker
        return current_set

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
        price = price[(price["date"] <= datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (price["date"] >= (datetime.strptime(self.start,"%Y-%m-%d")).replace(tzinfo=timezone.utc))]
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
        cleaned["wd"] = cleaned["wmtd"] - cleaned["weekly_price_regression_prediction"]
        cleaned["passed"] = [1 if x > 0 else 0 for x in cleaned["wd"]]
        cols = ["date","ticker","adjclose","wmtd"]
        cols.extend([x for x in cleaned.columns if "regression" in x or "classification" in x])
        return cleaned[cols]
    
    def merge_btc_weeklies(self,price,price_factors,price_results):
        price.reset_index(inplace=True)
        for column in price.columns:
            price.rename(columns={column:"".join(column.lower().replace("_","").split())},inplace=True)
        try:
            price["date"] = pd.to_datetime(price["date"],utc=True)
        except:
            price["date"] = price["date"]
        price.sort_values(by="date",ascending=True,inplace=True)
        price = price[(price["date"] > datetime.strptime(self.end,"%Y-%m-%d").replace(tzinfo=timezone.utc)) 
                            & (price["date"] <= (datetime.strptime(self.end,"%Y-%m-%d") + timedelta(days=7)).replace(tzinfo=timezone.utc))]
        for i in range(len(price_results)):
            price_result = price_results.iloc[i]
            price_model = price_result["model"]
            prediction = price_model.predict(price_factors[i]["X"])
            if price_result["classification"]:
                col_name = "classification"
            else:
                col_name = "regression"
            price["weekly_btc_{}_prediction".format(col_name)] = prediction[0]
            price["weekly_btc_{}_score".format(col_name)] = price_result["score"]
        cleaned = price
        cleaned["ticker"] = self.ticker
        wmtd = []
        for i in range(len(cleaned)):
            current_date = cleaned.iloc[i]["date"] 
            window = cleaned.iloc[0:i+1]
            max_val = window["adjclose"].max()
            wmtd.append(max_val)
        cleaned["wmtd"] = wmtd
        cleaned["wd"] = cleaned["wmtd"] - cleaned["weekly_btc_regression_prediction"]
        cleaned["passed"] = [1 if x > 0 else 0 for x in cleaned["wd"]]
        cols = ["date","ticker","adjclose","wmtd"]
        cols.extend([x for x in cleaned.columns if "regression" in x or "classification" in x])
        return cleaned[cols]
    
    
    def merge_weeklies_rec(self,price,price_factors,price_results):
        price.reset_index(inplace=True)
        package = {}
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
            package["weekly_price_{}_prediction".format(col_name)] = prediction[0]
            package["weekly_price_{}_score".format(col_name)] = price_result["score"]
            package["ticker"] = self.ticker
        cleaned = price
        window = cleaned.tail(5)
        wmtd = window["adjclose"].max()
        if wmtd > package["weekly_price_regression_prediction"]:
            package["wd"] = 1
        else:
            package["wd"] = 0
        package["wmtd"] = wmtd
        return package

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