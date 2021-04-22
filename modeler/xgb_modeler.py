from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
import pandas as pd
from datetime import datetime
import pickle
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from sklearn.linear_model import LinearRegression
import warnings
warnings.simplefilter(action='ignore', category=Warning)

import pickle
class Modeler(object):
    def __init__(self,ticker):
        self.ticker = ticker
        
    
    def model(self,data):
        params = {"booster":["gbtree", "gblinear", "dart"]}
        gs = GridSearchCV(xgb.XGBModel(objective="reg:squarederror"), param_grid=params,scoring="r2")
        gs.fit(data["X"],data["y"])
        model = pickle.dumps(gs.best_estimator_)
        return {"ticker":self.ticker,"model":model,"score":gs.best_score_}