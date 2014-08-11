#!/usr/bin/python
# -*- coding: utf-8 -*-

#import neede packages
import urllib2
from bs4 import BeautifulSoup
import re
import pandas as pd
import MySQLdb
import pandas.io.sql;


data_to_sql=pd.read_csv('/home/janne/toto/data_all.tsv',sep='\t',encoding='utf-8')

l= []
for i in data_to_sql['Kerroin'].values:
    l.append(str(i).replace(',','.'))
data_to_sql['Kerroin'] = l


l2=[]
for i in data_to_sql['EtuKengat'].values:
    if i == 'C':
        i='jalassa'
    else:
        i='pois'
    l2.append(i)
data_to_sql['EtuKengat'] =l2


l3=[]
for i in data_to_sql['TakaKengat'].values:
    if i == 'C':
        i='jalassa'
    else:
        i='pois'
    l3.append(i)
data_to_sql['TakaKengat'] =l3


l4=[]
#print data_to_sql['Jotain'].dtypes
for i in data_to_sql['Jotain'].values:
	#print (i,type(i))
	if isinstance(i,float):
		l4.append(i)
	else:
		l4.append(re.sub(r'\W',"",str(re.sub(r'\D\W',"",str(i.encode('utf-8'))))))
	#else:
	#	print i 
    #l4.append(str(i))
#print l4
data_to_sql['Jotain'] =l4

l5=[]
for i in data_to_sql['Palkinto'].values:
	if isinstance(i,float):
		l5.append(i)
	else:
		a=re.sub(r'\D', "",str(i[:-5].encode('utf-8')))
		l5.append(a)
		#print a
    #a=re.sub(r'\D', "",str(i[:-5].encode('utf-8')))
    #l5.append(a)
data_to_sql['Palkinto']=l5

#data_to_sql['Jotain'] =l4

# OPEN CONNECTION
# NOTE!!! PUT IN host AND passwd

db = MySQLdb.connect(host="", # your host, usually localhost
                     user="myuser", # your username
                      passwd="", # your password
                      db="TOTO") # name of the data base


data_to_sql.fillna('novalue',inplace=True)
import pandas.io.sql;

# WRITE TO SQL
#NOTE !! CHANGE if_exist to APPEND
print data_to_sql
#pandas.io.sql.write_frame(data_to_sql[[0,1,2,3,4,5,6,7]], 'toto', db, flavor='mysql', if_exists='replace')
"""
for i in data_to_sql[[3]].values:
	print i[0].encode('utf-8')
"""


