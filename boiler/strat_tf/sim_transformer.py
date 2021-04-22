import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np

class SimTransformer(object):        
    @classmethod
    def transform(self,date,ticker,data,predictions,results,shift):
        packages = []
        buy_date = date
        final_prediction = predictions[0][len(predictions[0])-1]
        sell_date = buy_date + timedelta(days=1)
        for i in range(len(final_prediction)):
            try:
                prediction = final_prediction[i]
                while buy_date.weekday() > 4:
                    buy_date = buy_date + timedelta(days=1)
                    sell_date = buy_date + timedelta(days=1)
                while sell_date.weekday() > 4:
                    sell_date = sell_date + timedelta(days=1)
                if len(data[data["date"]==sell_date]) == 1 and len(data[data["date"]==buy_date]) == 1:
                    sell_price = data[data["date"]==sell_date][ticker].item()
                    buy_price = data[data["date"]==buy_date][ticker].item()
                    actual_change = (sell_price - buy_price) / buy_price
                    predicted_change = (prediction - buy_price) / buy_price
                    changes = [1 + x if x > 0 else 1 - x for x in [actual_change,predicted_change]]
                    if actual_change < 0:
                        actualdailyreturn = -np.log(changes[0])/(i+1)
                    else:
                        actualdailyreturn = np.log(changes[0])/(i+1)    
                    if predicted_change < 0:
                        predicteddailyreturn = -np.log(changes[1])/(i+1)
                    else:
                        predicteddailyreturn = np.log(changes[1])/(i+1)
                    package = {"calculation_date":date,
                                "shift":shift,
                                "buy_date":buy_date,
                                "sell_date":sell_date,
                                "buy_price":buy_price,
                                "sell_price":sell_price, 
                                "prediction":prediction,
                                "predicteddailyreturn":predicteddailyreturn,
                                "actualdailyreturn":actualdailyreturn,
                                "mape":results["results"][1]/100,
                                "ticker":ticker}
                    packages.append(package)
                sell_date = sell_date + timedelta(days=1)
            except Exception as e:
                print(str(e))
                continue
        return packages

