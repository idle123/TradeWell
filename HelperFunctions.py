import pandas as pd
import numpy as np

def sepByOrderId( dfDay):
    dfDay.sort_values(by='Time',inplace=True)
    orderIds = dfDay['Order id'].unique()    
    resDf = pd.DataFrame(columns = ['OrderId','Date','MinTime','MaxTime','Symbol','Segment','AvgExecPx','ExecQty','TradeType'])
    for order in orderIds:
        dfOrder = dfDay.loc[dfDay['Order id'] == order]
        executedQty = dfOrder['Quantity'].sum()
        firstExec = dfOrder['Time'].min()
        lastExec = dfOrder['Time'].max()
        avgPx = (dfOrder['Quantity']*dfOrder['Trade Price (₹)']) .sum() / executedQty
        symbol = dfOrder['Symbol'].head(1).item()
        segment = dfOrder['Segment'].head(1).item()
        tradeType = dfOrder['Trade Type'].head(1).item()
        date = dfOrder['Date'].head(1).item()
        resDf.loc[len(resDf.index)] = [order,date,firstExec,lastExec,symbol,segment,avgPx,executedQty,tradeType] 
    return resDf

def enrichDF(orderData):
    res = pd.DataFrame();
    orderData.sort_values(by='MinTime',inplace=True)
    orderData['Sign'] = np.where(orderData['TradeType'] == 'Buy',1,-1)
    symbols = orderData['Symbol'].unique()
    for sym in symbols:
        symData = orderData.loc[orderData['Symbol'] == sym]
        symData.sort_values(by='MinTime',inplace=True)        
        lastQty = 0;
        lastDebit = 0;
        lastCredit = 0;
        lastRealizedPnL = 0;
        for index,row in symData.iterrows():
            avgPx = row['AvgExecPx']
            currQty = row['ExecQty']
            sign = row['Sign']
            orderId = row['OrderId']
            runningQty = lastQty+currQty*sign
            if(sign >0):
                symData.loc[symData['OrderId'] == orderId,['runningDebit']]= lastDebit+currQty*avgPx
                lastDebit= lastDebit+currQty*avgPx
            else:
                symData.loc[symData['OrderId'] == orderId,['runningCredit']]= lastCredit+currQty*avgPx
                lastCredit= lastCredit+currQty*avgPx
            if((abs(lastQty)>0 ) and (lastQty*runningQty <= 0)):
                #the sign of positions held has changed meaning some pnl would have been realized
                symData.loc[symData['OrderId'] == orderId,['realizedPnL']]= lastCredit - lastDebit + (runningQty)*avgPx- lastRealizedPnL
                lastRealizedPnL = lastCredit - lastDebit + (runningQty)*avgPx- lastRealizedPnL
            symData.loc[symData['OrderId'] == orderId,['runningQty']]= runningQty
            lastQty = runningQty
        if(res.empty):
            res = symData
        else:
            res = res.append(symData,ignore_index=True)
    return res        


def markRevengeTrades(orderData, pValue, tValue, coolOffValue):
    enrichedData  = enrichDF(orderData)
    markedData = pd.DataFrame()
    cutOff = pValue*tValue
    for sym in enrichedData['Symbol'].unique():
        symData = enrichedData.loc[enrichedData['Symbol'] == sym]
        symData.sort_values(by='MinTime',inplace=True)  
        maxDD = False
        lastRow  = 0
        for index,row in symData.iterrows():
            if(-1*row['realizedPnL'] >= cutOff):
                maxDD = True
                lastRow = row
                symData.loc[symData['OrderId'] == row['OrderId'],['maxDDBreached']] = True
                continue;
            if(maxDD):
                if(row['MinTime'] - lastRow['MinTime'] < coolOffValue):
                    cond1 = (lastRow['runningQty'] == 0) and (abs(row['runningQty']) > 0)
                    cond2 = (row['runningQty']*lastRow['runningQty'] < 0) or (abs(row['runningQty']) > abs(lastRow['runningQty']))
                    if((cond1) or (cond2)):
                        symData.loc[symData['OrderId'] == row['OrderId'],['isRevenge']] = True
                maxDD = False
        if(markedData.empty):
            markedData = symData
        else:
            markedData = markedData.append(symData, ignore_index = True)                    
    return markedData
        
 
def findTotalRevengeTradingLoss(markedDf):
    revengeFlag = False
    currSymbol = "None"
    totalLoss = 0
    totalGain = 0
    markedDf.sort_values(by=['Date','Symbol','MinTime','OrderId'],inplace=True)
    for index,row in markedDf.iterrows():
        if(row['isRevenge'] == True):
            revengeFlag = True
        if(revengeFlag and (abs(row['realizedPnL']) > 0)):
            revengeFlag = False
            if(row['realizedPnL'] < 0 ):
                totalLoss += row['realizedPnL']
            else:
                totalGain += row['realizedPnL']
    print("Your total loss due to revenge trading" + str(totalLoss))
    print("Your total gain due to revenge trading" + str(totalGain))
