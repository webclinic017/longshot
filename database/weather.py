from pymongo import MongoClient, DESCENDING
import pandas as pd
from datetime import timedelta
from database.abank import ABank

class Weather(ABank):
    def __init__(self):
        super().__init__("weather")
    