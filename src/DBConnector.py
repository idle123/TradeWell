# -*- coding: utf-8 -*-

import mysql.connector
import pandas as pd

def SQL_INSERT_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET):
    sql_texts = []
    for index, row in SOURCE.iterrows():       
        sql_texts.append('INSERT INTO '+TARGET+' ('+ str(', '.join(SOURCE.columns))+ ') VALUES '+ str(tuple(row.values)))        
    return sql_texts



def insertMethod(query):
    try:
        connection = mysql.connector.connect(host='localhost',
                                         database='TradeWell',
                                         user='root',
                                         password = "Synergize@22")
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print(cursor.rowcount, "Ordered inserted successfully into  DB")
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert order into DB {}".format(error))
        return(1)

    finally:
        if connection.is_connected():
            connection.close()
            return (0)
        
def getAllHistoricalOrders():
    try:
        connection = mysql.connector.connect(host='tradewell-db.cyvlqbncdlsw.ap-south-1.rds.amazonaws.com',
                                         database='tradewell_db',
                                         user='root',
                                         password = "myTradewell#123")
        #query = "select * from Orders where receivedTime > '" + date + "'"
        query = "select * from Orders"
        print(query)
        cursor = connection.cursor()
        cursor.execute(query)
        # get all records
        df = pd.DataFrame.from_records(cursor.fetchall(),
                               columns = [desc[0] for desc in cursor.description])
        print("Total number of rows in table: ", cursor.rowcount)
        cursor.close()
        return df
    except mysql.connector.Error as error:
        print("Failed to get orders from DB {}".format(error))
        return(1)