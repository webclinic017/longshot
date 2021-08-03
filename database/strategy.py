from pymongo import MongoClient, DESCENDING, ASCENDING
import pandas as pd
from datetime import timedelta
from database.abank import ABank

class Strategy(ABank):
    def __init__(self,database_name):
        super().__init__(database_name)