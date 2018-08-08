# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 13:10:32 2018

@author: AustinPeel
"""

from sqlConnect import mssql


#pull data
df =mssql.pull("DATA").data()

#pull columns
columns = mssql.pull("DATA").columns()

#send data, use actionType to drop append, truncate
mssql.send(df=df,tableName="DATA_BASE",actionType="append").data()

