
import pandas as pd
from datetime import datetime, timedelta

class DateTransformer(object):    
    @classmethod
    def convert_to_date(self,dataset,data,date_column):
        if "date" not in data.columns:
            data["date"] = [datetime.fromtimestamp(x) for x in data["t"]]
            data.rename(columns={"c":"adjclose"},inplace=True)
        try:
            data[date_column] = pd.to_datetime(data[date_column],utc=True)
        except:
            data[date_column] = data[date_column]
        return data