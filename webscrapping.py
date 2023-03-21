import requests
from requests_html import HTMLSession
import json
import sqlite3
from datetime import datetime


#CODE FOR SCRAPPING AND POPULATING DATABASE FOR JSE TICKER INFORMATION

#create database ('jse.db') and table for tickers (jse_equities)
#create scrapper for website jse.co.za to get listing of all tickers and companies on JSE
#split the data into ticker, yahoo ticker (i.e. ticker + '.JO') and name of company
#keep track of any failed additions by catching all exceptions and appending details to a failures list

conn = sqlite3.connect('jse.db')
c = conn.cursor()
with conn:
    c.execute("CREATE table jse_equities (ticker text, yticker text, name text)")

url = 'https://www.jse.co.za'
s = HTMLSession()
r = s.get(url)
r.html.render(sleep= 1)

failures = []
for index in range(1,343): 
#first ,<tr> tag was returning headers , all <tr> tags was returning other tickers including for commodities, preference shares, etc. (1,343) limited to JSE normal stock tickers    
    try:
        details = [i for i in r.html.find('tr')[index].text.split('\n')]
        ticker , name = details[0].split('-')[0] , ''.join(details[0].split('-')[1:]) #some companies had similar stock names. splitting on the '-' and taking only the second element resulted in similar names, and since the names are used in creating tables we ended up creating duplicate tables
        y_ticker = str(ticker + '.JO')
        with conn:
            c.execute('INSERT INTO jse_equities VALUES (?,?,?)', (ticker, y_ticker, name))
    except Exception:
        failures.append(('Index: {}'.format(index) , '{}'.format(r.html.find('tr')[index].text)))
        pass
print(failures)
print('complete')

conn.close()

#obtain all tickers from the database table jse_equities
tickers = []
conn = sqlite3.connect('jse.db')
with conn:
    c = conn.cursor()
    c.execute("SELECT * FROM jse_equities")
    for ticker in (c.fetchall()):
        tickers.append(ticker)
for i,j in enumerate(tickers):
    print(i,j)
#scrapper for stock trading history for all companies from 1 Jan 2002
#scrapping a chart required one to check the network tab and in this case arrange the elements by size
#the actual data was in the largest element
conn = sqlite3.connect('jse.db')
c = conn.cursor()
type_errors = []
key_errors = []
for index in range(len(tickers)):
    try:
        tbl_str = '''CREATE table "{}" (timestamp integer, 
                                        date text, 
                                        volume real, 
                                        low real, 
                                        high real, 
                                        open real, 
                                        close real)'''.format(tickers[index][2].replace(' ', ''))
        c.execute(tbl_str)  
        period1 = 1009868400
        period2 = 1643353200
        url = 'https://query1.finance.yahoo.com/v8/finance/chart/{}?symbol={}&period1={}&period2={}&useYfid=true&interval=1d&includePrePost=true&events=div|split|earn&lang=en-US&region=US&crumb=3o5Xf0xL5ZM&corsDomain=finance.yahoo.com'.format(tickers[index][1],tickers[index][1],period1,period2)
        headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'}
        data = {'method' : 'GET',
            'symbol' : tickers[index][1],
            'period1' : period1, #1 Jan 2002
            'period2' : period2, #28 Jan 2022
            'useYfid': True,
            'interval': '1d',
            'includePrePost': True,
            'events': 'div%7Csplit%7Cearn',
            'lang': 'en-US',
            'region': 'US',
            'crumb': '3o5Xf0xL5ZM',
            'corsDomain': 'finance.yahoo.com '}
        r = requests.get(url, headers = headers)
        cookies = r.cookies
        res = requests.get(url, data = data , headers = headers , cookies = cookies)
        res_json = res.json()
        time_stamp = res_json['chart']['result'][0]['timestamp']
        quote = res_json['chart']['result'][0]['indicators']['quote'][0] #returns a dictionary
        for t_index, _ in enumerate(time_stamp): #len(time_stamp)
            timestamp = time_stamp[t_index]
            date = datetime.date(datetime.fromtimestamp(timestamp))
            qoute_close = quote['close'][t_index] #accessing elements of qoute dictionary above using keys, returning a list and accessing the nth element of that list
            qoute_open = quote['open'][t_index] 
            qoute_low = quote['low'][t_index] 
            qoute_high = quote['high'][t_index] 
            qoute_volume = quote['volume'][t_index] 
            tbl_name = '{}'.format(tickers[index][2].replace(' ', ''))
            tbl_name_str = "INSERT INTO '{}' VALUES (?,?,?,?,?,?,?)".format(tbl_name) #(timestamp,date,qoute_volume,qoute_low,qoute_high,qoute_open,qoute_close)"
            c.execute(tbl_name_str,(timestamp,date,qoute_volume,qoute_low,qoute_high,qoute_open,qoute_close))
            conn.commit()
    except TypeError:
        type_errors.append(('{} : {}'.format(index, tickers[index])))
    except KeyError:
        key_errors.append(('{} : {}'.format(index, tickers[index])))
        pass
conn.close()

#way to execute strings with more than one variables was to create the strings outside in a normal format string statement and then pass the string into the c.execute function, refer to lines (8-15) , (46-48)
#manual update of database table
conn = sqlite3.connect('jse.db')
c = conn.cursor()
type_errors_update = []
key_errors_update = []
for index in range(282,(len(tickers)+1)):
    try:
        period1 = 1643612400
        period2 = 1644329700
        url = 'https://query1.finance.yahoo.com/v8/finance/chart/{}?symbol={}&period1={}&period2={}&useYfid=true&interval=1d&includePrePost=true&events=div|split|earn&lang=en-US&region=US&crumb=3o5Xf0xL5ZM&corsDomain=finance.yahoo.com'.format(tickers[index][1],tickers[index][1],period1,period2)
        headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'}
        data = {'method' : 'GET',
            'symbol' : tickers[index][1],
            'period1' : period1, #31 Jan 2022
            'period2' : period2, #8 Feb 2022
            'useYfid': True,
            'interval': '1d',
            'includePrePost': True,
            'events': 'div%7Csplit%7Cearn',
            'lang': 'en-US',
            'region': 'US',
            'crumb': '3o5Xf0xL5ZM',
            'corsDomain': 'finance.yahoo.com '}
        r = requests.get(url, headers = headers)
        cookies = r.cookies
        res = requests.get(url, data = data , headers = headers , cookies = cookies)
        res_json = res.json()
        time_stamp = res_json['chart']['result'][0]['timestamp']
        quote = res_json['chart']['result'][0]['indicators']['quote'][0] #returns a dictionary
        for t_index, _ in enumerate(time_stamp): #len(time_stamp)
            timestamp = time_stamp[t_index]
            date = datetime.date(datetime.fromtimestamp(timestamp))
            qoute_close = quote['close'][t_index] #accessing elements of qoute dictionary above using keys, returning a list and accessing the nth element of that list
            qoute_open = quote['open'][t_index] 
            qoute_low = quote['low'][t_index] 
            qoute_high = quote['high'][t_index] 
            qoute_volume = quote['volume'][t_index] 
            tbl_name = '{}'.format(tickers[index][2].replace(' ', ''))
            tbl_name_str = "INSERT INTO '{}' VALUES (?,?,?,?,?,?,?)".format(tbl_name) #(timestamp,date,qoute_volume,qoute_low,qoute_high,qoute_open,qoute_close)"
            c.execute(tbl_name_str,(timestamp,date,qoute_volume,qoute_low,qoute_high,qoute_open,qoute_close))
            conn.commit()
    except TypeError:
        type_errors_update.append(('{} : {}'.format(index, tickers[index])))
    except KeyError:
        key_errors_update.append(('{} : {}'.format(index, tickers[index])))
        pass
conn.close()
#way to execute strings with more than one variables was to create the strings outside in a normal format string statement and then pass the string into the c.execute function, refer to lines (8-15) , (46-48)
#function for adding individual companies by index number.
conn = sqlite3.connect('jse.db')
c = conn.cursor()
def create_populate_tbl(index):
    try:
        tbl_str = '''CREATE table "{}" (timestamp integer, 
                                        date text, 
                                        volume real, 
                                        low real, 
                                        high real, 
                                        open real, 
                                        close real)'''.format(tickers[index][2].replace(' ', ''))
        c.execute(tbl_str)  
        period1 = 1009868400
        period2 = 1643353200
        url = 'https://query1.finance.yahoo.com/v8/finance/chart/{}?symbol={}&period1={}&period2={}&useYfid=true&interval=1d&includePrePost=true&events=div|split|earn&lang=en-US&region=US&crumb=3o5Xf0xL5ZM&corsDomain=finance.yahoo.com'.format(tickers[index][1],tickers[index][1],period1,period2)
        headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'}
        data = {'method' : 'GET',
            'symbol' : tickers[index][1],
            'period1' : period1, #1 Jan 2002
            'period2' : period2, #28 Jan 2022
            'useYfid': True,
            'interval': '1d',
            'includePrePost': True,
            'events': 'div%7Csplit%7Cearn',
            'lang': 'en-US',
            'region': 'US',
            'crumb': '3o5Xf0xL5ZM',
            'corsDomain': 'finance.yahoo.com '}
        r = requests.get(url, headers = headers)
        cookies = r.cookies
        res = requests.get(url, data = data , headers = headers , cookies = cookies)
        res_json = res.json()
        time_stamp = res_json['chart']['result'][0]['timestamp']
        quote = res_json['chart']['result'][0]['indicators']['quote'][0] #returns a dictionary
        for t_index, _ in enumerate(time_stamp): #len(time_stamp)
            timestamp = time_stamp[t_index]
            date = datetime.date(datetime.fromtimestamp(timestamp))
            qoute_close = quote['close'][t_index] #accessing elements of qoute dictionary above using keys, returning a list and accessing the nth element of that list
            qoute_open = quote['open'][t_index] 
            qoute_low = quote['low'][t_index] 
            qoute_high = quote['high'][t_index] 
            qoute_volume = quote['volume'][t_index] 
            tbl_name = '{}'.format(tickers[index][2].replace(' ', ''))
            tbl_name_str = "INSERT INTO '{}' VALUES (?,?,?,?,?,?,?)".format(tbl_name) #(timestamp,date,qoute_volume,qoute_low,qoute_high,qoute_open,qoute_close)"
            c.execute(tbl_name_str,(timestamp,date,qoute_volume,qoute_low,qoute_high,qoute_open,qoute_close))
            conn.commit()
    except TypeError:
        type_errors.append(('{} : {}'.format(index, tickers[index])))
    except KeyError:
        key_errors.append(('{} : {}'.format(index, tickers[index])))
        pass
conn.close()
#calling the function above to create a table and entry of index 309
# conn = sqlite3.connect('jse.db')
# c = conn.cursor()
# create_populate_tbl(309)
# conn.close()
#correcting ticker symbol for The Foshini Group from TFGN to TFG in the database table
conn = sqlite3.connect('jse.db')
c = conn.cursor()
c.execute("""UPDATE jse_equities 
            SET ticker='TFG', yticker='TFG.JO' 
            WHERE ticker='TFGN'
          """)
conn.commit()
conn.close()
#code for dropping any tables to re-add them (if necessary)
# conn = sqlite3.connect('jse.db')
# c = conn.cursor()
# c.execute("DROP table TheFoschiniGroupLimitedNPL")
# conn.commit()
# conn.close()
#find empty tables in the database
conn = sqlite3.connect('jse.db')
c = conn.cursor()
with conn:
    c.execute("SELECT * FROM sqlite_master WHERE type='table';")
    list_tbls = (c.fetchall())

empty_tables = []
for entry in range(len(list_tbls)):
    with conn:
        c.execute('SELECT COUNT(*) FROM "{}"'.format(list_tbls[entry][1]))
        if c.fetchall()[0][0] == 0:
            empty_tables.append((entry,list_tbls[entry][1]))     
empty_tables
conn.close()