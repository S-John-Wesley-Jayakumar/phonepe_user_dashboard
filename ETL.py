
import pandas as pd
import json
import os
import git
import mysql.connector
from sqlalchemy import create_engine


def clone_repo_once(repo_url, dest_folder):
    if os.path.isdir(dest_folder):
        print(f"{dest_folder} already exists. Skipping clone.")
        return
    
    git.Repo.clone_from(repo_url, dest_folder)
    print(f"Cloned {repo_url} to {dest_folder}")


def extract_data_from_json_agg_bar(path,year,quater):

    with open(path, 'r') as f:
        data = json.load(f)

    names = []
    amounts = []
    count = []

    for transaction in data['data']['transactionData']:
        name = transaction['name']
        for instrument in transaction['paymentInstruments']:
            names.append(name)
            amounts.append(instrument['amount'])
            count.append(instrument['count'])

    df = pd.DataFrame({'T_type': names, 'Amount': amounts,'T_Count':count ,'Year':year,  'Quater':quater, 'chart_type':"agg-bar"})
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Sno'}, inplace=True)


    return df


def extract_data_from_json_agg_pie(path,year,quater):

    with open(path, 'r') as f:
        data = json.load(f)

    users_by_device = data['data']['usersByDevice']

    Reg_Users =[]
    Brand =[]
    Count =[]
    Percentage=[]

    for device in users_by_device:
        Reg_Users.append(data['data']['aggregated']['registeredUsers'])
        Brand.append( device['brand'])
        Count.append(device['count'])
        Percentage.append(device['percentage'])

        
    df = pd.DataFrame({'Reg_user':Reg_Users,'Brand':Brand,'User_Count':Count,'Percentage':Percentage,'Year':year,'Quater':quater, 'chart_type':"agg-pie" })
    # add a 'sno' column
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Sno'}, inplace=True)


    return df

def extract_data_from_json_map(json_file, year, quater):
    # Load the JSON data
    with open(json_file, 'r') as f:
        data = json.load(f)

    # Extract the name and amount data using a for loop
    names = []
    amounts = []
   
    amt=[]
    for item in data['data']['hoverDataList']:
        names.append(item['name'].title())
        amounts.append(item['metric'][0]['amount'])
        amt.append(int(((item['metric'][0]['amount'])-1)/1000000000))

    # Create a pandas DataFrame
    
    df = pd.DataFrame({'State': names, 'Amount': amounts,"Amt_color":amt, 'Year':year, 'Quater':quater,})
    df['Amount'] = df['Amount'].apply(lambda x: '{:.0f}'.format(x))
    df['ID_x'] = range(1, len(df) + 1)

    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Sno'}, inplace=True)

    return df





def combine_dataframes(df1, df2, df3, df4):
    df_combined = pd.concat([df1, df2, df3, df4], ignore_index=True)
    return df_combined







# DRIVER CODES OF FUNCTIONS

 #Clonng Git Repo
repo_path = "C:/Users/programms/Desktop/Phonepe_SQL_Dashboard/pulse1"

clone_repo_once(repo_path, "data")
#######################################################################


#driver codes


dfbarQ118 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2018\1.json', 2018, 'Q1')
dfbarQ218 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2018\2.json', 2018, 'Q2')
dfbarQ318 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2018\3.json', 2018, 'Q3')
dfbarQ418 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2018\4.json', 2018, 'Q4')

dfbarQ119 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2019\1.json', 2019, 'Q1')
dfbarQ219 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2019\2.json', 2019, 'Q2')
dfbarQ319 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2019\3.json', 2019, 'Q3')
dfbarQ419 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2019\4.json', 2019, 'Q4')

dfbarQ120= extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2020\1.json', 2020, 'Q1')
dfbarQ220 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2020\2.json', 2020, 'Q2')
dfbarQ320 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2020\3.json', 2020, 'Q3')
dfbarQ420 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2020\4.json', 2020, 'Q4')

dfbarQ121= extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2021\1.json', 2021, 'Q1')
dfbarQ221 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2021\2.json', 2021, 'Q2')
dfbarQ321 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2021\3.json', 2021, 'Q3')
dfbarQ421 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2021\4.json', 2021, 'Q4')

dfbarQ122= extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2022\1.json', 2022, 'Q1')
dfbarQ222 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2022\2.json', 2022, 'Q2')
dfbarQ322 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2022\3.json', 2022, 'Q3')
dfbarQ422 = extract_data_from_json_agg_bar(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\transaction\country\india\2022\4.json', 2022, 'Q4')

print("BAR CHART DATA  .................")

dffinialbar18 = combine_dataframes(dfbarQ118, dfbarQ218, dfbarQ318, dfbarQ418)
dffinialbar19 = combine_dataframes(dfbarQ119, dfbarQ219, dfbarQ319, dfbarQ419)
dffinialbar20 = combine_dataframes(dfbarQ120, dfbarQ220, dfbarQ320, dfbarQ420)
dffinialbar21 = combine_dataframes(dfbarQ121, dfbarQ221, dfbarQ321, dfbarQ421)
dffinialbar22 = combine_dataframes(dfbarQ122, dfbarQ222, dfbarQ322, dfbarQ422)
print(dffinialbar18.head())
print(dffinialbar18.tail())



print("...............................................................................................")


dfpieq118 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2018\1.json', 2018, 'Q1')
dfpieq218 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2018\2.json', 2018, 'Q2')
dfpieq318 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2018\3.json', 2018, 'Q3')
dfpieq418 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2018\4.json', 2018, 'Q4')

dfpieq119 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2019\1.json', 2019, 'Q1')
dfpieq219 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2019\2.json', 2019, 'Q2')
dfpieq319 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2019\3.json', 2019, 'Q3')
dfpieq419 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2019\4.json', 2019, 'Q4')



dfpieq120 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2020\1.json', 2020, 'Q1')
dfpieq220 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2020\2.json', 2020, 'Q2')
dfpieq320 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2020\3.json', 2020, 'Q3')
dfpieq420 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2020\4.json', 2020, 'Q4')

dfpieq121 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2021\1.json', 2021, 'Q1')
dfpieq221 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2021\2.json', 2021, 'Q2')
dfpieq321 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2021\3.json', 2021, 'Q3')
dfpieq421 = extract_data_from_json_agg_pie(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\aggregated\user\country\india\2021\4.json', 2021, 'Q4')


print("PIE CHART DATA  18.................")
dffinialpie18 = combine_dataframes(dfpieq118, dfpieq218, dfpieq318, dfpieq418)
dffinialpie19 = combine_dataframes(dfpieq119, dfpieq219, dfpieq319, dfpieq419)
dffinialpie20 = combine_dataframes(dfpieq120, dfpieq220, dfpieq320, dfpieq420)
dffinialpie21 = combine_dataframes(dfpieq121, dfpieq221, dfpieq321, dfpieq421)

print(dffinialpie18.head())
print(dffinialpie18.tail())

print("...............................................................................................")
dfmapq118 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2018\1.json', 2018 ,'Q1')
dfmapq218 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2018\2.json', 2018 ,'Q2')
dfmapq318 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2018\3.json', 2018 ,'Q3')
dfmapq418 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2018\4.json', 2018 ,'Q4')

dfmapq119 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2019\1.json', 2019 ,'Q1')
dfmapq219 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2019\2.json', 2019 ,'Q2')
dfmapq319 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2019\3.json', 2019 ,'Q3')
dfmapq419 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2019\4.json', 2019 ,'Q4')

dfmapq120 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2020\1.json', 2020 ,'Q1')
dfmapq220 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2020\2.json', 2020 ,'Q2')
dfmapq320 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2020\3.json', 2020 ,'Q3')
dfmapq420 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2020\4.json', 2020 ,'Q4')

dfmapq121 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2021\1.json', 2021 ,'Q1')
dfmapq221 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2021\2.json', 2021 ,'Q2')
dfmapq321 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2021\3.json', 2021 ,'Q3')
dfmapq421 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2021\4.json', 2021 ,'Q4')

dfmapq122 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2022\1.json', 2022 ,'Q1')
dfmapq222 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2022\2.json', 2022 ,'Q2')
dfmapq322 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2022\3.json', 2022 ,'Q3')
dfmapq422 = extract_data_from_json_map(r'C:\Users\programms\Desktop\SQL_git_phone_pe_pluse_DashApp\app\data\data\map\transaction\hover\country\india\2022\4.json', 2022 ,'Q4')




print("map DATA  18.................")
dffinialmap18 = combine_dataframes(dfmapq118, dfmapq218, dfmapq318, dfmapq418)
dffinialmap19 = combine_dataframes(dfmapq119, dfmapq219, dfmapq319, dfmapq419)
dffinialmap20 = combine_dataframes(dfmapq120, dfmapq220, dfmapq320, dfmapq420)
dffinialmap21 = combine_dataframes(dfmapq121, dfmapq221, dfmapq320, dfmapq421)
dffinialmap22 = combine_dataframes(dfmapq122, dfmapq222, dfmapq321, dfmapq422)

print(dffinialmap18.head())
print(dffinialmap18.tail())



print("...............................................................................................")



def add(df18,df19,df20,df21,df22):
    df = pd.concat([df18, df19,df20,df21,df22], ignore_index=True)
    return df

def add3(df18,df19,df20):
    df = pd.concat([df18, df19,df20], ignore_index=True)
    return df
def add1(df18,df19):
    df = pd.concat([df18, df19], ignore_index=True)
    return df



def load_df_to_mysql(df, db_name, table_name, host, user, password):   

    # Connect to the database
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()

    # Create the database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.execute(f"USE {db_name}")

    # Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create the table with the same table name , columns as the dataframe

    columns = ', '.join([f"{col} VARCHAR(255)" for col in df.columns])

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")

    # Insert the data into the table
    values = [tuple(row) for row in df.values]

    datapoints = ', '.join(['%s'] * len(df.columns))

    query = f"INSERT INTO {table_name} VALUES ({datapoints})"

    cursor.executemany(query, values)
  

    # Commit the changes and close the connection 
    conn.commit()
    conn.close()


def load_df_to_mysql_map(df, db_name, table_name, host, user, password):   
    # Connect to the database
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()

    # Create the database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.execute(f"USE {db_name}")
     # Drop the table if it exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create the table if it doesn't exist
    query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (  
                Sno INT(11) NOT NULL,              
                State VARCHAR(50) NOT NULL,
                Amount BIGINT(20) NOT NULL,
                Amt_color INT(11) NOT NULL,
                Year INT(11) NOT NULL,
                Quater VARCHAR(50) NOT NULL,
                ID_x INT(11) NOT NULL
        );
    '''

    cursor.execute(query)

     # Insert the data into the table
    values = [tuple(row) for row in df.values]

    datapoints = ', '.join(['%s'] * len(df.columns))

    query1 = f"INSERT INTO {table_name} VALUES ({datapoints})"

    cursor.executemany(query1, values)

    # Commit the changes and close the connection 
    conn.commit()
    conn.close()


#Driver code to load data frame to my sql without schema
print(" DATA LOADING  TO MYSQL .................")

print("LOADING  DATA TO MYSQL DB  18.................")
bar = add(dffinialbar18, dffinialbar19,dffinialbar20,dffinialbar21,dffinialbar22)
map = add(dffinialmap18,dffinialmap19,dffinialmap20,dffinialmap21,dffinialmap22)
#pie = add(dffinialpie18, dffinialpie19,dffinialpie20,dffinialpie21,dffinialpie22)
pie = add3(dffinialpie18, dffinialpie19,dffinialpie20)



load_df_to_mysql(bar , 'Phone_Pe_db', 'agg_data', 'localhost', 'root', 'password')
load_df_to_mysql(pie , 'Phone_Pe_db', 'pie_data', 'localhost', 'root', 'password')

#Driver code to load data frame to my sql with schema fo chloropleth 

load_df_to_mysql_map(map , 'Phone_Pe_db', 'map_data', 'localhost', 'root', 'password')

print(" DATA LOADING COMPLETE   18.................")
print("...............................................................................................")





