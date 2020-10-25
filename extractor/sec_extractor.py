from urllib.request import urlretrieve
from zipfile import ZipFile  
from extractor.iextractor import IExtractor
class SECExtractor(IExtractor):

    @classmethod
    def extract(year,quarter):
        try:
            url = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{}q{}.zip".format(year,quarter)
            filename = "./sec/{}q{}.zip".format(year,quarter)
            urlretrieve(url, filename)
            with ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall("./sec/{}q{}".format(year,quarter))
        except Exception as e:
            print(year,quarter,str(e))