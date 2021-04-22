from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
from datetime import datetime
import warnings
import tensorflow as tf
warnings.simplefilter(action='ignore', category=Warning)
tf.compat.v1.logging.set_verbosity(
    0
)
class TFModeler(object):
    def __init__(self,ticker,prediction_days):
        self.ticker = ticker
        self.params = {"booster":["gbtree", "gblinear", "dart"]}
        self.callbacks = tf.keras.callbacks.EarlyStopping(monitor="loss",patience=2,mode="min")
        self.
    
    def tf_regression_model(self,refined):
        model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(units = 256,activation="relu"),
        tf.keras.layers.Dense(units = 256,activation="relu"),
        tf.keras.layers.Dense(units = 1)
        # tf.keras.layers.LSTM(256,return_sequences=True),
        # tf.keras.layers.Dense(units=prediction_days)
        ])
        X_train, X_test, y_train, y_test = self.linear_split(refined)
        model.compile(loss=tf.losses.MeanAbsolutePercentageError(),
                    metrics=[tf.metrics.MeanAbsolutePercentageError()],
                    optimizer=tf.optimizers.Adam())
        model.fit(tf.stack(X_train),tf.stack(y_train),epochs=25,validation_split=0.20,use_multiprocessing=True,verbose=0,shuffle=False,callbacks=self.callbacks)
        results = model.evaluate(tf.stack(X_test),tf.stack(y_test))
        return {"model":model,"results":results}
    
    def tf_classification_model(self,refined):
        X_train, X_test, y_train, y_test = self.linear_split(refined)
        model = tf.keras.Sequential([
            tf.keras.layers.DenseFeatures(refined.columns),
            tf.keras.layers.Dense(128,activation="relu"),
            tf.keras.layers.Dense(128,activation="relu"),
            tf.keras.layers.Dropout(.1),
            tf.keras.layers.Dense(1)
        ])
        model.compile(optimizer="adam",
        loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
        metrics=["accuracy"])
        model.fit(tf.stack(X_train),tf.stack(y_train),epochs=25,validation_split=0.20,use_multiprocessing=True,verbose=0,shuffle=False,callbacks=self.callbacks)
        results = model.evaluate(tf.stack(X_test),tf.stack(y_test))
        return {"model":model,"results":results}

    def linear_split(self,data):
        split = 0.80
        X_train = data["X"][:int(len(data["X"])*split)]
        X_test = data["X"][int(len(data["X"])*split):]
        y_train = data["y"][:int(len(data["y"])*split)]
        y_test = data["y"][int(len(data["y"])*split):]
        return [X_train,X_test,y_train,y_test]
    
    def shuffle_split(self,data):
        return train_test_split(data["X"], data["y"],train_size=0.75, test_size=0.25, random_state=42)
    
    def true_accuracy(self,y_pred,y_test):
        return accuracy_score([1 if x > 0 else 0 for x in np.diff(y_test)],[1 if x > 0 else 0 for x in np.diff(y_pred)])

