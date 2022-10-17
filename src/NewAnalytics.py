# -*- coding: utf-8 -*-

import DBConnector
import datetime
import pandas as pd
import FyersEnums
import  numpy as np
import decimal 
import ipdb
import warnings
import traceback


dataPath = '/Users/akankshapatel/TradeWell/data/result/'
#warnings.filterwarnings("error")
def convertOrdersToTrades(orders,trades):
    lastFilled = 0
    firstOrder = orders.iloc[0]
    origType = firstOrder['type']
    for index,row in orders.iterrows():
        if(lastFilled == 0):
            if(row['filledQty'] > 0):
                #insert trade
                trades.loc[len(trades.index),:] = [row['time'], len(trades.index), row['ID'], row['EntryID'],row['symbol'],row['type'],row['side'],row['limitPrice']
                                                   ,row['stopPrice'],row['tradedPrice'],row['qty'],row['remainingQuantity'],row['filledQty'],origType]
                lastFilled = row['filledQty']
        if(row['filledQty'] > lastFilled):
            #file a trade
            trades.loc[len(trades.index),:] = [row['time'], len(trades.index), row['ID'], row['EntryID'],row['symbol'],row['type'],row['side'],row['limitPrice']
                                               ,row['stopPrice'],row['tradedPrice'],row['qty'],row['remainingQuantity'],row['filledQty']-lastFilled,origType]
            lastFilled = row['filledQty']
    return trades

def populateCurrentHoldingColumn(trades):
    trades['date'] = trades['time'].apply(lambda x:x.date())
    cols = list(trades.columns)
    cols.append('holding')
    result_trades = pd.DataFrame(columns=cols)
    uniqueDates = trades['date'].unique()
    for date in uniqueDates:
        tradesForTheDate = trades.loc[trades['date'] == date]
        uniqSymbols = tradesForTheDate['Symbol'].unique()
        holdings = dict.fromkeys(uniqSymbols, 0) 
        tradesForTheDate = tradesForTheDate.sort_values(by=['time','TradeID'], ascending=True)
        for index,row in tradesForTheDate.iterrows():
            pos = holdings[row['Symbol']] + row['side']*row['executedQty']
            row['holding'] = pos
            result_trades.loc[index,:] = row
            holdings[row['Symbol']] = pos
        
    return result_trades  

def settleTradesWithEachOther(trades):
    print("We are here")
    trades['exhaustedQty'] = 0
    trades['settledQty'] = 0
    trades['settledPrice'] = 0
    trades['exhaustedBy'] = ' '
    trades['settledBy'] = ' '
    trades['settlementOrder'] = -1
    cols = list(trades.columns)
    result_trades = pd.DataFrame(columns=cols)
    uniqueDates = trades['date'].unique()
    i=0
    for date in uniqueDates:
        tradesForTheDate = trades.loc[trades['date'] == date]
        uniqSymbols = tradesForTheDate['Symbol'].unique()
        print(uniqSymbols)
        for sym in uniqSymbols:
            trades_date_sym = tradesForTheDate.loc[tradesForTheDate['Symbol'] == sym]
            trades_date_sym = trades_date_sym.sort_values(by=['EntryID'], ascending=True)
            longTrades = trades_date_sym.loc[trades_date_sym['side'] == 1]
            shortTrades = trades_date_sym.loc[trades_date_sym['side'] == -1]
            for index,r in trades_date_sym.iterrows():  
                #ipdb.set_trace()
                trades_date_sym.loc[trades_date_sym.index == index,'settlementOrder'] = i
                i = i+1
                row = trades_date_sym.loc[trades_date_sym.index == index].iloc[0]
                if(row['exhaustedQty'] == row['executedQty']):
                    continue
                if((row['side'] == -1) and (row['executedQty'] > 0)):
                    #find next buy row with atleast this much amount of quantity
                    longs = longTrades.loc[longTrades['EntryID'] > row['EntryID']]
                    for index2,lRow in longs.iterrows():
                        longRow = longs.loc[longs.index == index2].iloc[0]
                        remainingQty = decimal.Decimal(float(row['executedQty']))- (decimal.Decimal(float(row['settledQty']))+decimal.Decimal(float(row['exhaustedQty'])))
                        if(remainingQty <= 0):
                            continue
                        avlblQty =  decimal.Decimal(float(longRow['executedQty']))- (decimal.Decimal(float(longRow['exhaustedQty'])) + decimal.Decimal(float(longRow['settledQty'])))
                        if(avlblQty <= 0):
                            continue
                        exQty = 0
                        if(avlblQty >= remainingQty):
                            longRow['exhaustedQty'] += remainingQty
                            exQty = remainingQty
                            a = decimal.Decimal(float(row['settledQty']))*row['settledPrice'] 
                            b = remainingQty*longRow['executedPrice']
                            c = row['settledQty'] + remainingQty
                            newPrice = (a+ b)/c
                            row['settledQty'] += remainingQty
                            row['settledPrice'] = newPrice  
                        else:
                            #now we just take part quantity from here
                            exQty = avlblQty
                            longRow['exhaustedQty'] += avlblQty
                            a1 = row['settledQty']*row['settledPrice']
                            b1= avlblQty*longRow['executedPrice']
                            c1 = row['settledQty'] + avlblQty
                            newPrice = ( a1 + b1)/(c1)
                            row['settledQty']+= avlblQty
                            row['settledPrice'] = newPrice
                        trades_date_sym.loc[index,'settledQty'] = row['settledQty']
                        trades_date_sym.loc[index,'settledPrice'] = row['settledPrice']
                        trades_date_sym.loc[index,'settledBy'] += ((str(longRow['TradeID'])) + " qty" +str(exQty)+",")
                        longs.loc[index2,'exhaustedQty'] = longRow['exhaustedQty']
                        longTrades.loc[index2,'exhaustedQty'] = longRow['exhaustedQty']    
                        trades_date_sym.loc[index2,'exhaustedQty'] = longRow['exhaustedQty']
                        trades_date_sym.loc[index2,'exhaustedBy'] += (str(row['TradeID'])+" qty "+str(exQty)+",")
                if((row['side'] == 1) and (row['executedQty'] > 0)):                   
                    #find next buy row with atleast this much amount of quantity
                    shorts = shortTrades.loc[shortTrades['EntryID'] > row['EntryID']]
                    for index2,sRow in shorts.iterrows():
                        shortRow = shorts.loc[shorts.index == index2].iloc[0]
                        remainingQty = decimal.Decimal(float(row['executedQty']))- (decimal.Decimal(float(row['settledQty'])) + decimal.Decimal(float(row['exhaustedQty'])))
                        avlblQty =  decimal.Decimal(float(shortRow['executedQty']))- (decimal.Decimal(float(shortRow['exhaustedQty'])) + decimal.Decimal(float(shortRow['settledQty']) ))
                        if(remainingQty <= 0):
                            continue
                        if(avlblQty <= 0):
                            continue
                        exQty = 0
                        if(avlblQty >= remainingQty):
                            exQty = remainingQty
                            shortRow['exhaustedQty'] += remainingQty
                            a = decimal.Decimal(float(row['settledQty']))*(row['settledPrice'] )
                            b = remainingQty*shortRow['executedPrice']
                            c = row['settledQty'] + remainingQty
                            newPrice = (a+ b)/c
                            row['settledQty'] += remainingQty
                            row['settledPrice'] = newPrice  
                        else:
                            #now we just take part quantity from here
                            exQty = avlblQty
                            shortRow['exhaustedQty'] += avlblQty
                            a1 = row['settledQty']*row['settledPrice']
                            b1= avlblQty*shortRow['executedPrice']
                            c1 = row['settledQty'] + avlblQty
                            newPrice = ( a1 + b1)/(c1)
                            row['settledQty']+= avlblQty
                            row['settledPrice'] = newPrice
                        trades_date_sym.loc[index,'settledQty'] = row['settledQty']
                        trades_date_sym.loc[index,'settledPrice'] = row['settledPrice']
                        trades_date_sym.loc[index,'settledBy'] += ((str(shortRow['TradeID'])) + " qty" +str(exQty)+",")
                        shorts.loc[index2,'exhaustedQty'] = shortRow['exhaustedQty']
                        shortTrades.loc[index2,'exhaustedQty'] = shortRow['exhaustedQty']    
                        trades_date_sym.loc[index2,'exhaustedQty'] = shortRow['exhaustedQty']
                        trades_date_sym.loc[index2,'exhaustedBy'] += (str(row['TradeID'])+" qty "+str(exQty)+",")
            result_trades = pd.concat([result_trades,trades_date_sym],ignore_index=True)
    return result_trades
                
                
                
        
      
def createJournal(orders,trades):
    f = open("OrderUpdates.txt", "a")  
    f.write("'EntryID','time','ID','side','ProductType','status','qty','filledQty','remainingQuantity','limitPrice','stopPrice','type','tradedPrice','symbol'")
    uniqorders = orders['ID'].unique()
    for order in uniqorders:
        orderUpdates = orders.loc[orders['ID'] == order]
        orderUpdates = orderUpdates.sort_values(by=['EntryID'], ascending=True)
        prevRow = []
        f.write('\n ***********************New Order Entered*******************\n')
        for index, row in orderUpdates.iterrows():
            changed = []
            if(len(prevRow) != 0):
                #figure out what change has happened and write it down
                if(prevRow['status'] != row['status']):
                    changed.append('OrderStatus')
                if(prevRow['qty'] != row['qty']):
                    changed.append('quantity')
                if(prevRow['filledQty'] != row['filledQty']):
                    changed.append('filledQuantity')
                if(prevRow['limitPrice'] != row['limitPrice']):
                    changed.append('limitPrice')
                if(prevRow['stopPrice'] != row['stopPrice']):
                    changed.append('stopPrice')
                if(prevRow['type'] != row['type']):
                    changed.append('orderType')
                if(len(changed) > 0):
                    f.write("\n"+ str(changed) + " has changed\n")
                    correspondingTrade = trades.loc[trades['EntryID'] == row['EntryID']]
                    if(len(correspondingTrade ) > 0):
                        f.write("\n Current holding of symbol "+ row['symbol'] + " is- " + str(correspondingTrade['holding'].iloc[0]))
                        f.write("\n")
            f.write(str(row[['EntryID','time','ID','side','ProductType','status','qty','filledQty','remainingQuantity','limitPrice','stopPrice','type','tradedPrice','symbol'
]]))
            f.write('\n')
            prevRow = row
    f.close()

def createJournalSymbolWise(orders,trades):
    f = open("OrderUpdatesSymbol.txt", "a")  
    f.write("'EntryID','time','ID','side','ProductType','status','qty','filledQty','remainingQuantity','limitPrice','stopPrice','type','tradedPrice','symbol'")
    orders['date'] = orders['time'].apply(lambda x:x.date())
    uniqDates = orders['date'].unique()
    for date in uniqDates:
        f.write("\n************For date  "+ str(date )+ "**************\n")
        order_date = orders.loc[orders['date'] == date]
        #for this date find all unique symbols
        uniqSyms = order_date['symbol'].unique()
        for sym in uniqSyms:
            f.write("\n************Lifecycle of "+ sym + "**************\n")
            order_sym = order_date.loc[order_date['symbol'] == sym]
            #for these symbols fine unique orders
            uniqorders = order_sym['ID'].unique()
            for order in uniqorders:
                orderUpdates = order_sym.loc[order_sym['ID'] == order]
                orderUpdates = orderUpdates.sort_values(by=['EntryID'], ascending=True)
                prevRow = []
                f.write('\n ***************New Order Entered*******************\n')
                for index, row in orderUpdates.iterrows():
                    changed = []
                    if(len(prevRow) != 0):
                        #figure out what change has happened and write it down
                        if(prevRow['status'] != row['status']):
                            changed.append('OrderStatus')
                        if(prevRow['qty'] != row['qty']):
                            changed.append('quantity')
                        if(prevRow['filledQty'] != row['filledQty']):
                            changed.append('filledQuantity')
                        if(prevRow['limitPrice'] != row['limitPrice']):
                            changed.append('limitPrice')
                        if(prevRow['stopPrice'] != row['stopPrice']):
                            changed.append('stopPrice')
                        if(prevRow['type'] != row['type']):
                            changed.append('orderType')
                        if(len(changed) > 0):
                            f.write("\n"+ str(changed) + " has changed\n")
                            correspondingTrade = trades.loc[trades['EntryID'] == row['EntryID']]
                            if(len(correspondingTrade ) > 0):
                                f.write("\n Current holding of symbol "+ row['symbol'] + " is- " + str(correspondingTrade['holding'].iloc[0]))
                                f.write("\n")
                    f.write(str(row[['EntryID','time','ID','side','ProductType','status','qty','filledQty','remainingQuantity','limitPrice','stopPrice','type','tradedPrice','symbol'
        ]]))
                    f.write('\n')
                    prevRow = row
    f.close()
                        
    
def preprocessData():
    orders= DBConnector.getAllHistoricalOrders()
    #Filter out orders that have not been acknowledged by exchange
    orders = orders[orders['orderDateTime'].notna()]
    orders['time'] = orders['orderDateTime'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
    #sort by orderDateTime
    orders = orders.sort_values(by=['EntryID'], ascending=True)
    orders['status'] = orders['status'].apply(lambda x:FyersEnums.OrderStatus(x))
    orders['type'] = orders['type'].apply(lambda x:FyersEnums.OrderType(x))
    uniqorders = orders['ID'].unique()
    trades = pd.DataFrame(columns=['time','TradeID','OrderID','EntryID','Symbol','OrderType','side','limitPrice','stopPrice','executedPrice','orderQty','remainingQty','executedQty','originalType'])
    for order in uniqorders:
        orderUpdates = orders.loc[orders['ID'] == order]
        orderUpdates = orderUpdates.sort_values(by=['EntryID'], ascending=True)
        trades = convertOrdersToTrades(orderUpdates,trades)
    trades = populateCurrentHoldingColumn(trades)
    result = {}
    result['Orders'] = orders
    result['Trades'] = trades
    return result

def enrichPnL(trades):
    trades['PNL'] = np.nan
    for index,row in trades.iterrows():
        if(row['settledQty'] > 0):
            trades.loc[index,'PNL'] = decimal.Decimal(row['side'])*row['settledQty']*(row['settledPrice'] - row['executedPrice'])
    return trades
            
    
result = preprocessData()
orders = result['Orders']
trades = result['Trades']
#createJournal(orders,trades)
#createJournalSymbolWise(orders,trades)
a = settleTradesWithEachOther(trades)
a= enrichPnL(a)
a['error'] = (a['executedQty'] < (a['settledQty'] + a['exhaustedQty']))
a['unfinished'] = (a['executedQty'] > (a['settledQty'] + a['exhaustedQty']))
resultfilename = dataPath+str(datetime.datetime.today().date())+"_analytics.csv"
a.to_csv(resultfilename)

    
    