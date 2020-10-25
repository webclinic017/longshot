from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_squared_log_error, accuracy_score
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from sklearn.linear_model import LinearRegression, SGDRegressor, RidgeCV, SGDClassifier, RidgeClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.naive_bayes import GaussianNB
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=Warning)
class Modeler(object):
    def __init__(self,ticker):
        self.ticker = ticker
        self.params = {"booster":["gbtree", "gblinear", "dart"]}
        
    def classify_wta(self,dataset,data,training_time,gap):
        sk_result = self.sk_classify_all(data)
        df = pd.DataFrame(sk_result)
        df["dataset"] = dataset
        df["training_time"] = training_time
        df["gap"] = gap
        df["classification"] = True
        return df.sort_values("score",ascending=False).iloc[0] 

    def model(self,dataset,data,training_time,gap):
        results = []
        sk_result = self.sk_model(data)
        results.extend(sk_result)
        df = pd.DataFrame(results)
        df["dataset"] = dataset
        df["training_time"] = training_time
        df["gap"] = gap
        df["classification"] = False
        return df.sort_values("score",ascending=False).iloc[0]
    
    def quarterly_model(self,dataset,data,training_time,gap):
        results = []
        sk_result = self.sk_quarterly_model(data)
        results.extend(sk_result)
        df = pd.DataFrame(results)
        df["dataset"] = dataset
        df["training_time"] = training_time
        df["gap"] = gap
        df["classification"] = False
        return df.sort_values("score",ascending=False).iloc[0]
    
    def sk_model(self,data):
        stuff = {"sgd" : SGDRegressor(fit_intercept=True),"r" : RidgeCV(fit_intercept=True),"lr" : LinearRegression(fit_intercept=True)}
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        for regressor in stuff:
            results = []
            model = stuff[regressor].fit(X_train,y_train)
            y_pred = model.predict(X_test)
            accuracy = r2_score(y_test,y_pred)
            result = {"model":model,"score":accuracy}
            results.append(result)
        return results

    def sk_quarterly_model(self,data):
        stuff = {"sgd" : SGDRegressor(),"r" : RidgeCV(),"lr" : LinearRegression()}
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        for regressor in stuff:
            results = []
            model = stuff[regressor].fit(X_train,y_train)
            y_pred = model.predict(X_test)
            accuracy = r2_score(y_test,y_pred)
            result = {"model":model,"score":accuracy}
            results.append(result)
        return results
    
    def sk_classify_all(self,data):
        results = []
        vc = VotingClassifier(estimators=[("sgdc" , SGDClassifier(early_stopping=True)),
                ("ridge" , RidgeClassifier()),
                ("tree",DecisionTreeClassifier()),
                ("neighbors",KNeighborsClassifier()),
                ("svc",SVC()),
                ("g",GaussianNB()),
                ("rfc",RandomForestClassifier())])
        models = {"sgdc" : SGDClassifier(early_stopping=True),
                "ridge" : RidgeClassifier(),
                "tree":DecisionTreeClassifier(),
                "neighbors":KNeighborsClassifier(),
                "svc":SVC(),
                "g":GaussianNB(),
                "rfc":RandomForestClassifier(),
                "vc":vc}
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        for classifier in models:
            try:
                model = models[classifier]
                model.fit(X_train,y_train)
                y_pred = model.predict(X_test)
                score = accuracy_score(y_test,y_pred)
                result = {"model":model,"score":score}
                results.append(result)
            except:
                continue
        return results
    
    def linear_split(self,data):
        split = 0.7
        X_train = data["X"][:int(len(data["X"])*split)]
        X_test = data["X"][int(len(data["X"])*split):]
        y_train = data["y"][:int(len(data["y"])*split)]
        y_test = data["y"][int(len(data["y"])*split):]
        return [X_train,X_test,y_train,y_test]
    
    def shuffle_split(self,data):
        return train_test_split(data["X"], data["y"],train_size=0.75, test_size=0.25, random_state=42)
    
    def true_accuracy(self,y_pred,y_test):
        return accuracy_score([1 if x > 0 else 0 for x in np.diff(y_test)],[1 if x > 0 else 0 for x in np.diff(y_pred)])

