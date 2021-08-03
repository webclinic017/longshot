from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.experimental import enable_halving_search_cv  # noqa
from sklearn.metrics import mean_squared_error, r2_score, mean_squared_log_error, accuracy_score, mean_absolute_percentage_error
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split, HalvingGridSearchCV
from sklearn.linear_model import LinearRegression, SGDRegressor, RidgeCV, SGDClassifier, RidgeClassifier, LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.multioutput import MultiOutputClassifier
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.naive_bayes import GaussianNB
import numpy as np
import pandas as pd
from datetime import datetime
import tensorflow as tf
import xgboost as xgb
import warnings
import os

from xgboost.sklearn import XGBClassifier
warnings.simplefilter(action='ignore', category=Warning)
tf.compat.v1.logging.set_verbosity(
    0
)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
class Modeler(object):
        
    @classmethod
    def classification(self,data,tf,deep,xgb,sk,multioutput):
        results = []
        data["y"] = data["y"].drop("y",axis=1,errors="ignore")
        if sk:
            sk_result = self.sk_model(data,deep)
            results.extend(sk_result)
        if xgb:
            xg_result = self.xgb_classify(data,multioutput)
            results.append(xg_result)
        if tf:
            tf_result = self.tf_classify(data)
            results.append(tf_result)
        df = pd.DataFrame(results)
        df["model_type"] = "classification"
        return df.sort_values("score",ascending=False).iloc[0]
    
    @classmethod
    def regression(self,data,ranked,tf,xgb,sk,deep):
        results = []
        data["y"] = data["y"].drop("y_class",axis=1,errors="ignore")
        if sk:
            sk_result = self.sk_model(data,deep)
            results.extend(sk_result)
        if xgb:
            xg_result = self.xgb_model(data)
            results.append(xg_result)
        if tf:
            tf_result = self.tf_model(data)
            results.append(tf_result)
        df = pd.DataFrame(results)
        if ranked:
            df["model_type"] = "ranked"
        else:
            df["model_type"] = "regression"
        return df.sort_values("score",ascending=False).iloc[0]
    
    @classmethod
    def xgb_model(self,data):
        try:
            params = {"booster":["gbtree", "gblinear", "dart"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            gs = GridSearchCV(xgb.XGBRegressor(objective="reg:squarederror" ), param_grid=params,scoring="r2")
            gs.fit(X_train,y_train)
            score = r2_score(gs.predict(X_test),y_test)
            model = gs.best_estimator_
            return {"api":"xgb","model":model,"score":score}
        except Exception as e:
            return {"api":"xgb","model":str(e),"score":9999}
    
    @classmethod
    def xgb_classify(self,data,multioutput):
        try:
            params = {"booster":["gbtree", "gblinear", "dart"]}
            X_train, X_test, y_train, y_test = self.shuffle_split(data)
            if multioutput:
                gs = MultiOutputClassifier(XGBClassifier(eval_metric="logloss"))
                gs.fit(X_train,y_train)
                model = gs
            else:
                gs = GridSearchCV(xgb.XGBClassifier(objective="binary:logistic",eval_metric = "logloss"), param_grid=params,scoring="accuracy")
                y_train = LabelEncoder().fit(y_train).transform(y_train)
                gs.fit(X_train,y_train)
                y_test = LabelEncoder().fit(y_test).transform(y_test)
                model = gs.best_estimator_
            score = accuracy_score(gs.predict(X_test),y_test)
            return {"api":"xgb","model":model,"score":score}
        except Exception as e:
            return {"api":"xgb","model":str(e),"score":-9999}
    
    @classmethod
    def sk_model(self,data,deep):
        stuff = {
            "sgd" : {"model":SGDRegressor(fit_intercept=True),"params":{"loss":["squared_loss","huber"]
                                                            ,"learning_rate":["constant","optimal","adaptive"]
                                                            ,"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
            "r" : {"model":RidgeCV(alphas=[0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1],fit_intercept=True),"params":{}},
            "lr" : {"model":LinearRegression(fit_intercept=True),"params":{"fit_intercept":[True,False]}}
        }
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        results = []
        for regressor in stuff:
            try:
                model = stuff[regressor]["model"].fit(X_train,y_train)
                params = stuff[regressor]["params"]
                if not deep:
                    model.fit(X_train,y_train)
                else:
                    gs = HalvingGridSearchCV(model,params,cv=10,scoring="neg_mean_squared_error")
                    gs.fit(X_train,y_train)
                    model = gs.best_estimator_
                y_pred = model.predict(X_test)
                accuracy = r2_score(y_test,y_pred)
                result = {"api":"skl","model":model,"score":accuracy}
                results.append(result)
            except Exception as e:
                print(str(e))
                result = {"api":"skl","model":str(e),"score":-99999}
                results.append(result)
                continue
        return results

    @classmethod
    def sk_classify(self,data,deep,multioutput):
        results = []
        vc = VotingClassifier(estimators=[("sgdc" , SGDClassifier(early_stopping=True)),
                ("ridge" , RidgeClassifier()),
                ("tree",DecisionTreeClassifier()),
                ("neighbors",KNeighborsClassifier()),
                ("svc",SVC()),
                ("g",GaussianNB()),
                ("rfc",RandomForestClassifier())])
        models = {"sgdc" : {"model":SGDClassifier(),"params":{"loss":["hinge","log","perceptron"]
                                                                                ,"learning_rate":["constant","optimal","adaptive"]
                                                                                ,"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
                "ridge" : {"model":RidgeClassifier(),"params":{"alpha" : [0.0001,0.001, 0.01, 0.1, 0.2, 0.5, 1]}},
                "tree":{"model":DecisionTreeClassifier(),"params":{'max_depth': range(1,11)}},
                "neighbors":{"model":KNeighborsClassifier(),"params":{'knn__n_neighbors': range(1, 10)}},
                "svc":{"model":SVC(),"params":{"kernel":["linear","poly","rbf"],"C":[0.001,0.01,0.1,1,10],"gamma":[0.001,0.01,0.1,1]}},
                "g":{"model":GaussianNB(),"params":{}},
                "rfc":{"model":RandomForestClassifier(),"params":{"criterion":["gini","entropy"]
                                                                ,"n_estimators":[100,150,200]
                                                                ,"max_depth":[None,1,3,5,10]
                                                                ,"min_samples_split":[5,10]
                                                                ,"min_samples_leaf":[5,10]}},
                "vc":vc}
        X_train, X_test, y_train, y_test = self.shuffle_split(data)
        for classifier in models:
            try:
                model = models[classifier]["model"]
                params = models[classifier]["params"]
                if classifier == "vc" or not deep or not multioutput:
                    model.fit(X_train,y_train)
                if multioutput:
                    model = MultiOutputClassifier(model).fit(X_train,y_train)
                else:
                    gs = HalvingGridSearchCV(model,params,cv=5,scoring="accuracy")
                    gs.fit(X_train,y_train)
                    model = gs.best_estimator_
                y_pred = model.predict(X_test)
                score = accuracy_score(y_test,y_pred)
                result = {"api":"skl","model":model,"score":score}
                results.append(result)
            except:
                continue
        return results
    
    @classmethod
    def tf_model(self,refined):
        tf.keras.backend.set_floatx('float64')
        callbacks = tf.keras.callbacks.EarlyStopping(monitor="loss",patience=5,mode="min")
        try:
            model = tf.keras.models.Sequential([
            tf.keras.layers.Dense(units = 256,activation="relu"),
            tf.keras.layers.Dense(units = 256,activation="relu"),
            tf.keras.layers.Dense(units = 1)
            # tf.keras.layers.Dense(units = 256,activation="relu"),
            # tf.keras.layers.LSTM(256,return_sequences=True),
            # tf.keras.layers.Dense(1)
            ])
            X_train, X_test, y_train, y_test = self.shuffle_split(refined)
            model.compile(loss=tf.losses.MeanAbsolutePercentageError(),
                        metrics=[tf.metrics.MeanAbsolutePercentageError()],
                        optimizer=tf.optimizers.Adam())
            model.fit(tf.stack(X_train),tf.stack(y_train),epochs=50,validation_split=0.20,use_multiprocessing=True,verbose=0,shuffle=False,callbacks=callbacks)
            results = model.evaluate(tf.stack(X_test),tf.stack(y_test))
            return {"api":"tf","model":model,"score":results[1]/100}
        except Exception as e:
            return {"api":"tf","model":str(e),"score":-9999}
    
    @classmethod
    def tf_classify(self,refined):
        callbacks = tf.keras.callbacks.EarlyStopping(monitor="loss",patience=2,mode="min")
        try:
            X_train, X_test, y_train, y_test = self.shuffle_split(refined)
            model = tf.keras.Sequential([
                # tf.keras.layers.DenseFeatures(X_train.columns),
                tf.keras.layers.Dense(128,activation="relu"),
                tf.keras.layers.Dense(128,activation="relu"),
                tf.keras.layers.Dropout(.1),
                tf.keras.layers.Dense(1)
            ])
            model.compile(optimizer="adam",
            loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
            metrics=["accuracy"])
            model.fit(tf.stack(X_train),tf.stack(y_train),epochs=50,validation_split=0.20,use_multiprocessing=True,verbose=0,shuffle=False,callbacks=callbacks)
            results = model.evaluate(tf.stack(X_test),tf.stack(y_test))
            return {"api":"tf","model":model,"score":results[1]}
        except Exception as e:
            return {"api":"tf","model":str(e),"score":-9999}

    @classmethod
    def linear_split(self,data):
        split = 0.7
        X_train = data["X"][:int(len(data["X"])*split)]
        X_test = data["X"][int(len(data["X"])*split):]
        y_train = data["y"][:int(len(data["y"])*split)]
        y_test = data["y"][int(len(data["y"])*split):]
        return [X_train,X_test,y_train,y_test]
    
    @classmethod
    def shuffle_split(self,data):
        return train_test_split(data["X"], data["y"],train_size=0.75, test_size=0.25, random_state=42)
    
    @classmethod
    def true_accuracy(self,y_pred,y_test):
        return accuracy_score([1 if x > 0 else 0 for x in np.diff(y_test)],[1 if x > 0 else 0 for x in np.diff(y_pred)])

