from sklearn.preprocessing import Normalizer
from sklearn.pipeline import Pipeline
import numpy as np
import pandas as pd
from datetime import datetime
import math
class Preprocessor(object):
    def __init__(self,ticker):
        self.ticker = ticker
    
    def fundamental_preprocess(self,data):
        drop_columns = ["year","ticker","adjclose","index"]
        data.fillna(0,inplace=True)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        features = data.drop(drop_columns,axis=1,errors="ignore").copy()
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        return {"X":processed,"y":data["adjclose"]}

    def preprocess_regression(self,data,ticker,batch_size,prediction_days):
        data.fillna(0,inplace=True)
        data.rename(columns={"adjclose":"y"},inplace=True)
        data["y"] = data["y"].shift(-1)
        data = data[:-1]
        data.reset_index(inplace=True)
        features = data.drop(["date","y","_id","index","year","ticker","level_0"],axis=1,errors="ignore").copy().astype(np.float32)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        plz = []
        y_plz = []
        y_pivots = []
        for i in range(0,len(processed)-batch_size-prediction_days):
            plz.append(processed.iloc[i:i+batch_size])
            y_pivots.append(data["y"].iloc[i+batch_size-1])
            y_plz.append([data["y"].iloc[i+batch_size:i+batch_size+prediction_days]])
        # y_plz = [[[(np.log(1+(value - y_pivots[i])/y_pivots[i])/(i+1)) if (value - y_pivots[i])/y_pivots[i] > 0 else 
        #             (-np.log(1-(value - y_pivots[i])/y_pivots[i])/(i+1)) for value in x] for x in y_plz[i]] for i in range(len(y_plz))]
        return {"X":plz[1:],"y":y_plz[1:]} 
    
    def preprocess_price_regression(self,data,ticker,batch_size,prediction_days,shift):
        data.fillna(0,inplace=True)
        data.reset_index(inplace=True)
        features = data.drop(["date","label_date","y","_id","index","year","ticker","level_0"],axis=1,errors="ignore").copy().astype(np.float32)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        plz = []
        y_plz = []
        y_pivots = []
        for i in range(1,len(processed)-batch_size-prediction_days):
            plz.append(processed.iloc[i:i+batch_size].values)
            y_pivots.append(data["y"].iloc[i+batch_size-1])
            y_plz.append([data["y"].iloc[i+batch_size:i+batch_size+prediction_days].values])
        # y_plz = [[[(np.log(1+(value - y_pivots[i])/y_pivots[i])/(i+1)) * 1000 if (value - y_pivots[i])/y_pivots[i] > 0 else 
        #             (-np.log(1-(value - y_pivots[i])/y_pivots[i])/(i+1)) * 1000 for value in x] for x in y_plz[i]] for i in range(len(y_plz))]
        return {"X":plz[1:],"y":y_plz[1:]} 

    def preprocess_prediction(self,data):
        data.fillna(0,inplace=True)
        data.rename(columns={"adjclose":"y"},inplace=True)
        data.reset_index(inplace=True)
        features = data.drop(["date","label_date","y","_id","index","year","ticker","level_0"],axis=1,errors="ignore").copy().astype(np.float32)
        return features.values