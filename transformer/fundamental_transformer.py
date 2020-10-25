import pandas as pd
from datetime import datetime, timedelta

class FundamentalTransformer(object):    
    @classmethod
    def transform(self,values,num):
        table = num.pivot_table(values="value",columns="tag",index=["adsh"])
        table.reset_index(inplace=True)
        table = table.merge(values,on="adsh",how="left")
        table.drop("_id",axis=1,inplace=True)
        table.reset_index(inplace=True)
        for column in table.columns:
            if "." in column:
                new = column.replace(".","-")
                table.rename(columns={column:new},inplace=True)
        table.drop("index",axis=1,inplace=True)
        return table