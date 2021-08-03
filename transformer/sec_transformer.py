from pandas import read_csv

class SECTransformer(object):    
    def __init__(self,year,quarter):
        self.year = year
        self.quarter = quarter

    def transform(self,file_prefix):
        path = "./sec/{}q{}/{}.txt".format(self.year,self.quarter,file_prefix)
        try:
            data = read_csv(path,engine="c",sep="\t",error_bad_lines=False,low_memory=False)
        except Exception as e:
            data = read_csv(path,engine="c",sep="\t",error_bad_lines=False,encoding = "ISO-8859-1",low_memory=False)
        data["year"] = self.year
        data["quarter"] = self.quarter
        return data