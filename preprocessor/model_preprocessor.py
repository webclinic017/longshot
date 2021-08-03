from sklearn.preprocessing import Normalizer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
import numpy as np
import pandas as pd
from datetime import datetime
class ModelPreprocessor(object):
    
    @classmethod
    def preprocess(self,data):
        """
        Attrs:
        1) data: data in format of features and one column defined as y
        """
        drop_columns = ["date","year","quarter","week","ticker","y","y_class"]
        num_pipeline = Pipeline([
            ('normalizer',Normalizer())
            ])
        features = data.drop(drop_columns,axis=1,errors="ignore").copy()
        features.fillna(-99999,inplace=True)
        for column in features:
            missing_rate = len(features[features[column] ==-99999]) / len(features)
            if missing_rate > 0.1:
                features.drop(column,axis=1,inplace=True)
        features.replace(-99999,np.NaN,inplace=True)
        si = SimpleImputer(strategy="mean")
        cleaned = si.fit_transform(features)
        c = pd.DataFrame(cleaned,columns=features.columns)
        features = c.copy()
        processed = pd.DataFrame(num_pipeline.fit_transform(features),columns=features.columns,index=features.index)
        return {"X":processed,"y":data[["y","y_class"]]}