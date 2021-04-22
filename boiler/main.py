from database.strategy import Strategy
from database.sec import SEC
from database.market import Market
from utils.date_utils import DateUtils
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import math
from ibapi.contract import Contract
from trader.IBApp import IBApp
import time
import asyncio

app = IBApp()
async def main():
    await app.connect("127.0.0.1",7497,clientId=0)
    contract = Contract()
    contract.symbol = "AAPL"
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"
    contract.primaryExchange = "NASDAQ"
    app.reqMarketDataType(4)
    await app.reqMarketData(1,contract,"",False,False,[])
    app.disconnect()
asyncio.run(main())