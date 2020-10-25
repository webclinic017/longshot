from sklearn.preprocessing import Normalizer
from sklearn.pipeline import Pipeline
import numpy as np
import pandas as pd
from datetime import datetime
import pickle
class PredictorPreprocessor(object):
    def __init__(self,ticker):
        self.ticker = ticker
    
    def fundamental_preprocess(self,data):
        drop_columns = ["quarter","year","ticker","adjclose"]
        features = data.drop(drop_columns,axis=1,errors="ignore").copy()
        features.fillna(0,inplace=True)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        return {"X":processed,"y":data["adjclose"]}

    def preprocess_regression(self,data):
        features = data.copy().astype(np.float32)
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        return {"X":processed} 
    
    def preprocess_classify(self,data):
        features = data.copy().astype(np.float32)
        return {"X":features}