from transformer.itransformer import ITransformer

class ATransformer(ITransformer):
    def __init__(self,ticker):
        self.ticker = ticker

    def transform(self,data):
        return data
