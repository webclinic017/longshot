from sklearn.preprocessing import Normalizer
from sklearn.pipeline import Pipeline
import numpy as np
import pandas as pd
from datetime import datetime
class ModelPreprocessor(object):
    def __init__(self,ticker):
        self.ticker = ticker
    
    def fundamental_preprocess(self,data):
        drop_columns = ["quarter","year","ticker","adjclose","index"]
        data.fillna(0,inplace=True)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        features = data.drop(drop_columns,axis=1,errors="ignore").copy()
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        return {"X":processed,"y":data["adjclose"]}

    def day_trade_preprocess_regression(self,data,ticker):
        data.fillna(0,inplace=True)
        features = data.drop(["date","y","_id","index","quarter","year"],axis=1,errors="ignore").copy().astype(np.float32)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        return {"X":processed,"y":data["y"]} 
    
    def day_trade_preprocess_classify(self,data,ticker):
        data.fillna(0,inplace=True)
        features = data.drop(["date","y","_id","index","quarter","year"],axis=1,errors="ignore").copy().astype(np.float32)
        return {"X":features,"y":data["y"]} 
    
    def trade_signal_preprocess_classify(self,data):
        data.fillna(0,inplace=True)
        features = data.drop(["date","ticker","y","_id","index"],axis=1,errors="ignore").copy().astype(np.float32)
        return {"X":features,"y":data["y"]} 
    
    def trade_signal_preprocess_regression(self,data):
        data.fillna(0,inplace=True)
        features = data.drop(["date","ticker","y","_id","index"],axis=1,errors="ignore").copy().astype(np.float32)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        return {"X":processed,"y":data["y"]} 