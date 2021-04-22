import pandas as pd

class ColumnTransformer(object):    
    @classmethod
    def rename_columns(self,data,gap):
        for column in data.columns:
            data.rename(columns={column:"".join(column.lower().replace(gap,"").split())},inplace=True)
        return data

    @classmethod
    def drop_irrelevant_columns(self,data):
        drop_columns = []
        for column in data.columns:
            if len([x for x in data.columns if x == column]) > 1:
                drop_columns.append(column)
        data.drop(drop_columns,axis=1,inplace=True)
        return data