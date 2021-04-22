from pymongo import MongoClient, DESCENDING
import pandas as pd
from datetime import timedelta
from database.abank import ABank

class Merrill(ABank):
    def __init__(self):
        super().__init__("merrill")