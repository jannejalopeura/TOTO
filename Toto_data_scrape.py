#!/usr/bin/python
# -*- coding: utf-8 -*-

#import neede packages
import urllib2
from bs4 import BeautifulSoup
import re
import pandas as pd
import MySQLdb
import pandas.io.sql;



#raw data from horse list

dataRaw1 = BeautifulSoup(urllib2.urlopen('https://heppa.hippos.fi/heppa/racing/RaceHorsesAll.html'))


#scrape horse id and name 

listOfHorseId=[]
listOfHorseIdandName=[]
for line in dataRaw1('div',{'class':'data'})[0].findAll('a',href=True):
    #print line
    #print re.findall('sp=([^&]*)',line['href'])
    l = re.findall('sp=([^&]*)',line['href'])
    #print line.getText().strip()
    l2=  line.getText().strip()
    l.append(l2)
    #print l
    #print line.a.string
    #print l
    if len(l) ==3:
        l_new = [l[0]]+[l[2]]
        #print l_new
        listOfHorseIdandName.append(l_new)
        listOfHorseId.append(l[0])
    #print type(re.findall('sp=([^&]*)',a['href'])) list

# using horseId scrape racing history from html. Save it to local directory
# NOTE !!! CHANGE DIRECOTRIE AND FILE NAMES !!!!!#


import pandas as pd
data_to_sql = pd.DataFrame()
dataRacingHistoryRaw1 = []
#listOfHorseId = [listOfHorseId[1],listOfHorseId[0]]
print listOfHorseId
for HorseId in listOfHorseId:
    print HorseId
    RacingDataRaw=BeautifulSoup(urllib2.urlopen('http://heppa.hippos.fi/heppa/horse/RacingHistory,$HorseSearchResult.$HorseLink.$DirectLink.sdirect?sp='+str(HorseId)+'&sp=X'))
    print 'http://heppa.hippos.fi/heppa/horse/RacingHistory,$HorseSearchResult.$HorseLink.$DirectLink.sdirect?sp='+str(HorseId)+'&sp=X'
    HeaderRow=['HorseId','Name']
    try:
        for header in RacingDataRaw('table',{'class':'sortable'})[1].findAll('tr')[0].findAll('th'):
        #print header.getText().strip()
            HeaderRow.append(header.getText().strip())
        Rows = []
        dataLen= len(RacingDataRaw('table',{'class':'sortable'})[1].findAll('tr'))
        dataRange = [i+1 for i in range(dataLen-1)]
        for i in dataRange:
            row =  RacingDataRaw('table',{'class':'sortable'})[1].findAll('tr')[i].findAll('td')
            Row = [HorseId,dict(listOfHorseIdandName)[HorseId]]
            for column in row:
                Row.append(column.getText().strip())
            Rows.append(Row)
    except:
        for header in RacingDataRaw('table',{'class':'sortable'})[0].findAll('tr')[0].findAll('th'):
        #print header.getText().strip()
            HeaderRow.append(header.getText().strip())
        Rows = []
        dataLen= len(RacingDataRaw('table',{'class':'sortable'})[0].findAll('tr'))
        dataRange = [i+1 for i in range(dataLen-1)]
        for i in dataRange:
            row =  RacingDataRaw('table',{'class':'sortable'})[0].findAll('tr')[i].findAll('td')
            Row = [HorseId,dict(listOfHorseIdandName)[HorseId]]
            for column in row:
                Row.append(column.getText().strip())
            Rows.append(Row)
    data_to_sql_HorseId = pd.DataFrame(Rows)
    data_to_sql_HorseId.to_csv('/home/janne/toto/data_1.tsv',header=False,index=False,sep='\t',mode='a',encoding='utf-8')
    #print data_to_sql_HorseId
    data_to_sql=data_to_sql.append(data_to_sql_HorseId,ignore_index=True)
    #print data_to_sql
#print data_to_sql
data_to_sql.columns=['HorseId','Name','Paikka','Date','Lahtö','Rata','Matka','Tyyppi','Aika','Sija','Jotain','Kerroin','Palkinto','Ohjastaja','SE','EtuKengat','TakaKengat','Selostus']
print data_to_sql.dtypes
data_to_sql.to_csv('/home/janne/toto/data_all.tsv',index=False,sep='\t',encoding='utf-8')

            
    #for i in range(dateLen):         
    #for range(dataLen)
    #for column in RacingDataRaw('table',{'class':'sortable'})[1].findAll('tr').findAll('td'):
    #    print column


#Change data to fit it to the sql:       
l= []
for i in data_to_sql['Kerroin'].values:
    l.append(i.replace(',','.'))
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
for i in data_to_sql['Jotain'].values:
    l4.append(i[0:3])
#print l4
data_to_sql['Jotain'] =l4

l5=[]
for i in data_to_sql['Palkinto'].values:

    a=re.sub(r'\D', "",str(i[:-5].encode('utf-8')))
    l5.append(a)
data_to_sql['Palkinto']=l5
#data_to_sql['Jotain'] =l4

# OPEN CONNECTION
# NOTE!!! PUT IN host AND passwd
db = MySQLdb.connect(host="", # your host, usually localhost
                     user="myuser", # your username
                      passwd="", # your password
                      db="TOTO") # name of the data base


import pandas.io.sql;

# WRITE TO SQL
#NOTE !! CHANGE if_exist to APPEND
pandas.io.sql.write_frame(data_to_sql[[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]], 'toto', db, flavor='mysql', if_exists='replace')


