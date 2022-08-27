# -*- coding: utf-8 -*-
import pandas as pd
import os
import glob
#import matplotlib.pyplot as plt
from datetime import timedelta
from HelperFunctions import *
import json


def plotSymbolTrajectory(dfDay):
    symbols  = dfDay['Symbol'].unique()
    for sym in symbols:
        dfSym = dfDay.loc[df['Symbol'] ==  sym]
        dfSym.sort_values(by=['Time'],inplace=True)
        dfSym.loc[dfSym['Trade Type'] == 'Buy','ntnlSign'] = -1
        dfSym.loc[dfSym['Trade Type'] == 'Sell','ntnlSign'] = 1
        dfSym['SignedNotional'] = dfSym['Trade Price (₹)']*dfSym['Quantity']*dfSym['ntnlSign']
        dfSym['cumSignedNotional'] = dfSym['SignedNotional'].cumsum()
        #plt.plot(dfSym['Time'], dfSym['cumSignedNotional'])
        #plt.title(date+"   "+ sym)
        #plt.show()

            


def processTradeDataOfUser():
        finalres = pd.DataFrame()        
        path = os.getcwd() + "\\..\\data\\trade\\"
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        orderDf = pd.DataFrame(columns = ['OrderId','Date','MinTime','MaxTime','Symbol','Segment','AvgExecPx','ExecQty','TradeType'])
        for f in csv_files:
            df = pd.read_csv(f)
            pd.to_datetime(df['Date'])
            dates = df['Date'].unique()
            print("max date is ", dates.max())
            print("min date is", dates.min())
            for date in dates:
                dfDay = df.loc[df['Date'] == date]
                dfDay['Time']= dfDay['Date'] + " " + dfDay['Time']
                dfDay['Time'] = pd.to_datetime(dfDay['Time'])
                orderDf_date = sepByOrderId(dfDay)
                res= markRevengeTrades(orderDf_date, 70000,0.02,timedelta( seconds=300))
                if(orderDf.empty):
                    orderDf = orderDf_date
                else:
                    orderDf = orderDf.append(orderDf_date,ignore_index=True)
                if(finalres.empty):
                    finalres = res
                else:
                    finalres = finalres.append(res,ignore_index=True)    
        finalres.to_csv("tempFinalRes.csv")
        orderDf.to_csv("orderDf.csv")
        findTotalRevengeTradingLoss(finalres)

#Read JSON file that contains order info
#def readOrderInfo_json():
def readOrderJSON():
    path = os.getcwd() + "\\..\\data\\Orders\\"
    json_files = glob.glob(os.path.join(path,"*.json"))
    j =1 
    orderPlacementData = pd.DataFrame()
    for f in json_files:
        file = open(f)
        j = json.load(file)
        if(j['code'] == 200):
            orders = j['orderBook']
            newdf = pd.DataFrame(orders)
            if(orderPlacementData.empty):
                orderPlacementData = newdf
            else:
                orderPlacementData = orderPlacementData.append(newdf,ignore_index=True)
    return orderPlacementData

def readTradeJSON():
    path = os.getcwd() + "\\..\\data\\tradeJSON\\"
    json_files = glob.glob(os.path.join(path,"*.json"))
    j =1 
    tradeData = pd.DataFrame()
    for f in json_files:
        file = open(f)
        j = json.load(file)
        if(j['code'] == 200):
            trades = j['tradeBook']
            newdf = pd.DataFrame(trades)
            if(tradeData.empty):
                tradeData = newdf
            else:
                tradeData = tradeData.append(newdf,ignore_index=True)
    return tradeData

tradeData = readTradeJSON()
orderData = readOrderJSON()