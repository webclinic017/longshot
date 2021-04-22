from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract 
from ibapi.ticktype import TickTypeEnum
from ibapi.order import Order
from time import sleep
class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
    
    def make_contract(self,symbol,sec_type,exchange,primary_exch,currency):
        cont = Contract()
        cont.symbol = symbol
        cont.secType = sec_type
        cont.exchange = exchange
        cont.primaryExch = primary_exch
        cont.currency = currency
        return cont

    def make_order(self,action,quantity,price = None):
        if price != None:
            order = Order()
            order.orderType = 'LMT'
            order.totalQuantity = quantity
            order.action = action
            order.lmtPrice = price
        else:
            order = Order()
            order.orderType = 'MKT'
            order.totalQuantity = quantity      
            order.action = action
        if action == "BUY":
            order.tif = "DAY"
        else:
            order.tif = "GTC"
        return order
    def positionEnd(self):
        print("end, disconnecting")
        self.disconnect()

    def execute_order(self,oid,symbol,action,quantity,price):
        curr = "USD"
        sec_type = "STK"
        exchange = "SMART"
        primary_exchange = "SMART"
        cont = self.make_contract(symbol,sec_type,exchange,primary_exchange,curr)
        offer = self.make_order(action,quantity,price)
        try:
            self.placeOrder(oid,cont,offer)
        except Exception as e:
            print(str(e))
        sleep(1)