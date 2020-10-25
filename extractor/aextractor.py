from extractor.iextractor import IExtractor
from requests import get
class AExtractor(IExtractor):
    
    def extract(self,api,params):
        return requests.get("https://github.com/dev-ejc/")