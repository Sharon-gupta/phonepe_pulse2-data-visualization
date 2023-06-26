import os
import json
import pandas as pd
path1 = "C:/Sample/pulse/data/aggregated/transaction/country/india/state/"
agg_trans_list = os.listdir(path1)
column1 = {'state':[],'year':[],'quater':[],'transaction_type':[],'transaction_count':[],'transaction_amount':[]}
for state in agg_trans_list:
    path_state = path1 + state + "/"
    agg_yr_list = os.listdir(path_state)
    for year in agg_yr_list:
        path_year = path_state + year + "/"
        agg_file_list = os.listdir(path_year)
        for file in agg_file_list:
            path_file = path_year + file
            data = open(path_file, 'r')
            D = json.load(data)
            try:
                 for i in D['data']['transactionData']:
                     Name = i['name']
                     count = i['paymentInstruments'][0]['count']
                     amount = i['paymentInstruments'][0]['amount']
                     column1['transaction_type'].append(Name)
                     column1['transaction_count'].append(count)
                     column1['transaction_amount'].append(amount)
                     column1['state'].append(state)
                     column1['year'].append(year)
                     column1['quater'].append(int(file.strip('.json')))
            except:
                pass
df_agg_trans = pd.DataFrame(column1)
#print(df_agg_trans)
df_agg_trans.to_csv('agg_trans.csv')

path2 = "C:/Sample/pulse/data/aggregated/user/country/india/state/"
agg_user_list = os.listdir(path2)
column2 = {'state': [], 'year': [], 'quater': [], 'brands': [],'count': [], 'percentage': []}
for state in agg_user_list:
     path_state = path2 + state + "/"
     agg_yr_list = os.listdir(path_state)
     for year in agg_yr_list:
         path_year = path_state + year + "/"
         agg_file_list = os.listdir(path_year)
         for file in agg_file_list:
             path_file = path_year + file
             data = open(path_file, 'r')
             D = json.load(data)
             try:
                 for i in D['data']["usersByDevice"]:
                     brand_name = i['brand']
                     counts = i['count']
                     percents = i['percentage']
                     column2['brands'].append(brand_name)
                     column2['count'].append(counts)
                     column2['percentage'].append(percents)
                     column2['state'].append(state)
                     column2['year'].append(year)
                     column2['quater'].append(int(file.strip('.json')))

             except:
                 pass

df_agg_user = pd.DataFrame(column2)
#print(df_agg_user)
df_agg_user.to_csv('agg_user.csv')


path3 = "C:/Sample/pulse/data/map/transaction/hover/country/india/state/"
map_trans_list = os.listdir(path3)
column3 = {'state': [], 'year': [], 'quater': [], 'district': [],'count': [], 'amount': []}
for state in map_trans_list:
     path_state = path3 + state + "/"
     map_yr_list = os.listdir(path_state)
     for year in map_yr_list:
         path_year = path_state + year + "/"
         map_file_list = os.listdir(path_year)
         for file in map_file_list:
             path_file = path_year + file
             data = open(path_file, 'r')
             D = json.load(data)
             try:
                 for i in D['data']['hoverDataList']:
                     district = i['name']
                     count = i['metric'][0]['count']
                     amount = i['metric'][0]['amount']
                     column3['district'].append(district)
                     column3['count'].append(count)
                     column3['amount'].append(amount)
                     column3['state'].append(state)
                     column3['year'].append(year)
                     column3['quater'].append(int(file.strip('.json')))
             except:
                 pass

df_map_trans = pd.DataFrame(column3)
#print(df_map_trans)
df_map_trans.to_csv('map_trans.csv')

path4 = "C:/Sample/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path4)
column4 = {"state": [], "year": [], "quater": [], "district": [],"registered_user": [], "appopens": []}
for state in map_user_list:
    path_state = path4 + state + "/"
    map_year_list = os.listdir(path_state)
    for year in map_year_list:
        path_year = path_state + year + "/"
        map_file_list = os.listdir(path_year)
        for file in map_file_list:
            path_file = path_year + file
            data = open(path_file, 'r')
            D = json.load(data)
            try:
              for i in D["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appOpens = i[1]['appOpens']
                column4["district"].append(district)
                column4["registered_user"].append(registereduser)
                column4["appopens"].append(appOpens)
                column4['state'].append(state)
                column4['year'].append(year)
                column4['quater'].append(int(file.strip('.json')))
            except:
                pass

df_map_user = pd.DataFrame(column4)
#print(df_map_user)
df_map_user.to_csv('map_user.csv')

path5 = "C:/Sample/pulse/data/top/transaction/country/india/state/"
top_trans_list = os.listdir(path5)
column5 = {'state': [], 'year': [], 'quater': [], 'pincode': [], 'transaction_count': [],'transaction_amount': []}

for state in top_trans_list:
    path_state = path5 + state + "/"
    top_year_list = os.listdir(path_state)

    for year in top_year_list:
        path_year = path_state + year + "/"
        top_file_list = os.listdir(path_year)

        for file in top_file_list:
            path_file = path_year + file
            data = open(path_file, 'r')
            D = json.load(data)

            try:
             for i in D['data']['pincodes']:
                name = i['entityName']
                count = i['metric']['count']
                amount = i['metric']['amount']
                column5['pincode'].append(name)
                column5['transaction_count'].append(count)
                column5['transaction_amount'].append(amount)
                column5['state'].append(state)
                column5['year'].append(year)
                column5['quater'].append(int(file.strip('.json')))
            except:
                pass

df_top_trans = pd.DataFrame(column5)
#print(df_top_trans)
df_top_trans.to_csv('top_trans.csv')

path6 = "C:/Sample/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)
column6 = {'state': [], 'year': [], 'quater': [], 'pincode': [],'registered_users': []}

for state in top_user_list:
    path_state = path6 + state + "/"
    top_year_list = os.listdir(path_state)

    for year in top_year_list:
        path_year = path_state + year + "/"
        top_file_list = os.listdir(path_year)

        for file in top_file_list:
            path_file = path_year + file
            data = open(path_file, 'r')
            D = json.load(data)
            try:
             for i in D['data']['pincodes']:
                name = i['name']
                registeredUsers = i['registeredUsers']
                column6['pincode'].append(name)
                column6['registered_users'].append(registeredUsers)
                column6['state'].append(state)
                column6['year'].append(year)
                column6['quater'].append(int(file.strip('.json')))
            except:
                pass
df_top_user = pd.DataFrame(column6)
#print(df_top_user)
df_top_user.to_csv('top_user.csv')

#Inserting data in sql
import pandas as pd
import mysql.connector
conn=mysql.connector.connect(user='root', password='12345', host='localhost',port=3306,auth_plugin='mysql_native_password')
cursor=conn.cursor()
cursor.execute("use phonepe_pulse")
# cursor.execute("create table agg_trans (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")
# for i,row in df_agg_trans.iterrows():
#       sql = "insert into agg_trans values (%s,%s,%s,%s,%s,%s)"
#       cursor.execute(sql, tuple(row))
#       conn.commit()
#
# cursor.execute("create table agg_user (State varchar(100), Year int, Quarter int, Brands varchar(100), Count int, Percentage double)")
# for i,row in df_agg_user.iterrows():
#       sql = "insert into agg_user values (%s,%s,%s,%s,%s,%s)"
#       cursor.execute(sql, tuple(row))
#       conn.commit()
#
# cursor.execute("create table map_trans (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount double)")
# for i,row in df_map_trans.iterrows():
#       sql = "insert into map_trans values (%s,%s,%s,%s,%s,%s)"
#       cursor.execute(sql, tuple(row))
#       conn.commit()
#
# cursor.execute("create table map_user (State varchar(100), Year int, Quarter int, District varchar(100), Registered_user int, App_opens int)")
# for i,row in df_map_user.iterrows():
#       sql = "insert into map_user values (%s,%s,%s,%s,%s,%s)"
#       cursor.execute(sql, tuple(row))
#       conn.commit()
#
# cursor.execute("create table top_trans (State varchar(100), Year int, Quarter int, Pincode int, Transaction_count int, Transaction_amount double)")
# for i,row in df_top_trans.iterrows():
#       sql = "INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)"
#       cursor.execute(sql, tuple(row))
#       conn.commit()
#
# cursor.execute("create table top_user (State varchar(100), Year int, Quarter int, Pincode int, Registered_users int)")
# for i,row in df_top_user.iterrows():
#       sql = "insert into top_user values (%s,%s,%s,%s,%s)"
#       cursor.execute(sql, tuple(row))
#       conn.commit()
#
# cursor.execute("show tables")
#print(cursor.fetchall())

df_agg_trans.to_csv('agg_trans.csv',index=False)
df_agg_user.to_csv('agg_user.csv',index=False)
df_map_trans.to_csv('map_trans.csv',index=False)
df_map_user.to_csv('map_user.csv',index=False)
df_top_trans.to_csv('top_trans.csv',index=False)
df_top_user.to_csv('top_user.csv',index=False)







