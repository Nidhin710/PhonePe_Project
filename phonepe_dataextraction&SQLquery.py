import os
import json
import pandas as pd
import psycopg2

def clean_states_column(dataframe, column_name="States"):
    dataframe[column_name] = dataframe[column_name].astype(str)
    
    dataframe[column_name] = dataframe[column_name].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
    dataframe[column_name] = dataframe[column_name].str.replace("-", " ")
    dataframe[column_name] = dataframe[column_name].str.title()
    dataframe[column_name] = dataframe[column_name].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
    
    return dataframe


#aggregated insurance_data
insurance_path = "phonepe/data/aggregated/insurance/country/india/state/"
insurance_state_list = os.listdir(insurance_path)

insurance_columns = {"States":[], "Years":[], "Quarter":[], "Insurance_type":[], "Insurance_count":[], "Insurance_amount":[]}

for state in insurance_state_list:
    state_path = os.path.join(insurance_path, state)
    
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)

        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            
            with open(file_path, "r") as data:
                insurance_json = json.load(data)

            for item in insurance_json["data"]["transactionData"]:
                insurance_columns["Insurance_type"].append(item["name"])
                insurance_columns["Insurance_count"].append(item["paymentInstruments"][0]["count"])
                insurance_columns["Insurance_amount"].append(item["paymentInstruments"][0]["amount"])
                insurance_columns["States"].append(state)
                insurance_columns["Years"].append(year)
                insurance_columns["Quarter"].append(int(file.strip(".json")))

insurance_df = pd.DataFrame(insurance_columns)
insurance_df = clean_states_column(insurance_df)

# transaction_data
transaction_path = "phonepe/data/aggregated/transaction/country/india/state/"
transaction_state_list = os.listdir(transaction_path)

transaction_columns = {"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}

for state in transaction_state_list:
    state_path = os.path.join(transaction_path, state)
    
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)

        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            
            with open(file_path, "r") as data:
                transaction_json = json.load(data)

            for item in transaction_json["data"]["transactionData"]:
                transaction_columns["Transaction_type"].append(item["name"])
                transaction_columns["Transaction_count"].append(item["paymentInstruments"][0]["count"])
                transaction_columns["Transaction_amount"].append(item["paymentInstruments"][0]["amount"])
                transaction_columns["States"].append(state)
                transaction_columns["Years"].append(year)
                transaction_columns["Quarter"].append(int(file.strip(".json")))

transaction_df = pd.DataFrame(transaction_columns)
transaction_df = clean_states_column(transaction_df)

# user_data
user_path = "phonepe/data/aggregated/user/country/india/state/"
user_state_list = os.listdir(user_path)

user_columns = {"States":[], "Years":[], "Quarter":[], "Brands":[], "UserCount":[], "UserPercentage":[]}

for state in user_state_list:
    state_path = os.path.join(user_path, state)
    
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        
        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            
            with open(file_path, "r") as data:
                user_json = json.load(data)

            try:
                for item in user_json["data"]["usersByDevice"]:
                    user_columns["Brands"].append(item["Brands"])
                    user_columns["UserCount"].append(item["count"])
                    user_columns["UserPercentage"].append(item["percentage"])
                    user_columns["States"].append(state)
                    user_columns["Years"].append(year)
                    user_columns["Quarter"].append(int(file.strip(".json")))
            except:
                pass

user_df = pd.DataFrame(user_columns)
user_df = clean_states_column(user_df)

# map_insurance_data
map_insurance_path = "phonepe/data/map/insurance/hover/country/india/state/"
map_insurance_state_list = os.listdir(map_insurance_path)

map_insurance_columns = {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}

for state in map_insurance_state_list:
    state_path = os.path.join(map_insurance_path, state)
    
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)

        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            
            with open(file_path, "r") as data:
                map_insurance_json = json.load(data)

            for item in map_insurance_json["data"]["hoverDataList"]:
                map_insurance_columns["Districts"].append(item["name"])
                map_insurance_columns["Transaction_count"].append(item["metric"][0]["count"])
                map_insurance_columns["Transaction_amount"].append(item["metric"][0]["amount"])
                map_insurance_columns["States"].append(state)
                map_insurance_columns["Years"].append(year)
                map_insurance_columns["Quarter"].append(int(file.strip(".json")))

map_insurance_df = pd.DataFrame(map_insurance_columns)
map_insurance_df = clean_states_column(map_insurance_df)

# map_transaction_data
map_transaction_path = "phonepe/data/map/transaction/hover/country/india/state/"
map_transaction_state_list = os.listdir(map_transaction_path)

map_transaction_columns = {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}

for state in map_transaction_state_list:
    state_path = os.path.join(map_transaction_path, state)
    
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        
        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            with open(file_path, "r") as data:
                map_transaction_json = json.load(data)

            for item in map_transaction_json['data']["hoverDataList"]:
                map_transaction_columns["Districts"].append(item["name"])
                map_transaction_columns["Transaction_count"].append(item["metric"][0]["count"])
                map_transaction_columns["Transaction_amount"].append(item["metric"][0]["amount"])
                map_transaction_columns["States"].append(state)
                map_transaction_columns["Years"].append(year)
                map_transaction_columns["Quarter"].append(int(file.strip(".json")))

map_transaction_df = pd.DataFrame(map_transaction_columns)
map_transaction_df = clean_states_column(map_transaction_df)

# map_user_data
map_user_path = "phonepe/data/map/user/hover/country/india/state/"
map_user_state_list = os.listdir(map_user_path)

map_user_columns = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

for state in map_user_state_list:
    state_path = os.path.join(map_user_path, state)
    
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        
        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            with open(file_path, "r") as data:
                map_user_json = json.load(data)

            for item in map_user_json["data"]["hoverData"].items():
                map_user_columns["Districts"].append(item[0])
                map_user_columns["RegisteredUser"].append(item[1]["registeredUsers"])
                map_user_columns["AppOpens"].append(item[1]["appOpens"])
                map_user_columns["States"].append(state)
                map_user_columns["Years"].append(year)
                map_user_columns["Quarter"].append(int(file.strip(".json")))

map_user_df = pd.DataFrame(map_user_columns)
map_user_df = clean_states_column(map_user_df)

# top_insurance_data
top_insurance_path = "phonepe/data/top/insurance/country/india/state/"
top_insurance_state_list = os.listdir(top_insurance_path)

top_insurance_columns = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_insurance_state_list:
    state_path = os.path.join(top_insurance_path, state)

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)

        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            with open(file_path, "r") as data:
                top_insurance_json = json.load(data)

            for item in top_insurance_json["data"]["pincodes"]:
                top_insurance_columns["Pincodes"].append(item["entityName"])
                top_insurance_columns["Transaction_count"].append(item["metric"]["count"])
                top_insurance_columns["Transaction_amount"].append(item["metric"]["amount"])
                top_insurance_columns["States"].append(state)
                top_insurance_columns["Years"].append(year)
                top_insurance_columns["Quarter"].append(int(file.strip(".json")))

top_insurance_df = pd.DataFrame(top_insurance_columns)
top_insurance_df = clean_states_column(top_insurance_df)

# top_transaction_data
top_transaction_path = "phonepe/data/top/transaction/country/india/state/"
top_transaction_state_list = os.listdir(top_transaction_path)

top_transaction_columns = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_transaction_state_list:
    state_path = os.path.join(top_transaction_path, state)
    
    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        
        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            with open(file_path, "r") as data:
                top_transaction_json = json.load(data)

            for item in top_transaction_json["data"]["pincodes"]:
                top_transaction_columns["Pincodes"].append(item["entityName"])
                top_transaction_columns["Transaction_count"].append(item["metric"]["count"])
                top_transaction_columns["Transaction_amount"].append(item["metric"]["amount"])
                top_transaction_columns["States"].append(state)
                top_transaction_columns["Years"].append(year)
                top_transaction_columns["Quarter"].append(int(file.strip(".json")))

top_transaction_df = pd.DataFrame(top_transaction_columns)
top_transaction_df = clean_states_column(top_transaction_df)

# top_user_data
top_user_path = "phonepe/data/top/user/country/india/state/"
top_user_state_list = os.listdir(top_user_path)

top_user_columns = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for state in top_user_state_list:
    state_path = os.path.join(top_user_path, state)

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)

        for file in os.listdir(year_path):
            file_path = os.path.join(year_path, file)
            with open(file_path, "r") as data:
                top_user_json = json.load(data)

            for item in top_user_json["data"]["pincodes"]:
                top_user_columns["Pincodes"].append(item["name"])
                top_user_columns["RegisteredUser"].append(item["registeredUsers"])
                top_user_columns["States"].append(state)
                top_user_columns["Years"].append(year)
                top_user_columns["Quarter"].append(int(file.strip(".json")))

top_user_df = pd.DataFrame(top_user_columns)
top_user_df = clean_states_column(top_user_df)


# pgsql connection
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="nidhin123",
    database="phonepe_data",
    port="5432"
)
cursor = mydb.cursor()



# Create table queries
table_queries = [
    '''
    CREATE TABLE if not exists aggregated_insurance (
        States varchar(50),
        Years int,
        Quarter int,
        Insurance_type varchar(50),
        Insurance_count bigint,
        Insurance_amount bigint
    )
    ''',
    '''
    CREATE TABLE if not exists aggregated_transaction (
        States varchar(50),
        Years int,
        Quarter int,
        Transaction_type varchar(50),
        Transaction_count bigint,
        Transaction_amount bigint
    )
    ''',
    '''
    CREATE TABLE if not exists aggregated_user (
        States varchar(50),
        Years int,
        Quarter int,
        Brands varchar(50),
        Transaction_count bigint,
        Percentage float
    )
    ''',
    '''
    CREATE TABLE if not exists map_insurance (
        States varchar(50),
        Years int,
        Quarter int,
        Districts varchar(50),
        Transaction_count bigint,
        Transaction_amount float
    )
    ''',
    '''
    CREATE TABLE if not exists map_transaction (
        States varchar(50),
        Years int,
        Quarter int,
        Districts varchar(50),
        Transaction_count bigint,
        Transaction_amount float
    )
    ''',
    '''
    CREATE TABLE if not exists map_user (
        States varchar(50),
        Years int,
        Quarter int,
        Districts varchar(50),
        RegisteredUser bigint,
        AppOpens bigint
    )
    ''',
    '''
    CREATE TABLE if not exists top_insurance (
        States varchar(50),
        Years int,
        Quarter int,
        Pincodes int,
        Transaction_count bigint,
        Transaction_amount bigint
    )
    ''',
    '''
    CREATE TABLE if not exists top_transaction (
        States varchar(50),
        Years int,
        Quarter int,
        Pincodes int,
        Transaction_count bigint,
        Transaction_amount bigint
    )
    ''',
    '''
    CREATE TABLE if not exists top_user (
        States varchar(50),
        Years int,
        Quarter int,
        Pincodes int,
        RegisteredUser bigint
    )
    '''
]

try:
    for query in table_queries:
        cursor.execute(query)
    mydb.commit()
    print("Tables created successfully.")
except Exception as e:
    print(f"Error creating tables: {e}")

# Insert queries for each DataFrame
try:
    for index, row in insurance_df.iterrows():
        insurance_query = '''
        INSERT INTO aggregated_insurance (States, Years, Quarter, Insurance_type, Insurance_count, Insurance_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row["States"],
            row["Years"],
            row["Quarter"],
            row["Insurance_type"],
            row["Insurance_count"],
            row["Insurance_amount"]
        )
        cursor.execute(insurance_query, values)
    mydb.commit()
    print("Data inserted into aggregated_insurance successfully.")
except Exception as e:
    print(f"Error inserting data into aggregated_insurance: {e}")

try:
    for index, row in transaction_df.iterrows():
        transaction_query = '''
        INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row["States"],
            row["Years"],
            row["Quarter"],
            row["Transaction_type"],
            row["Transaction_count"],
            row["Transaction_amount"]
        )
        cursor.execute(transaction_query, values)
    mydb.commit()
    print("Data inserted into aggregated_transaction successfully.")
except Exception as e:
    print(f"Error inserting data into aggregated_transaction: {e}")

try:
    for index, row in user_df.iterrows():
        user_query = '''
        INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row["States"],
            row["Years"],
            row["Quarter"],
            row["Brands"],
            row["Transaction_count"],
            row["Percentage"]
        )
        cursor.execute(user_query, values)
    mydb.commit()
    print("Data inserted into aggregated_user successfully.")
except Exception as e:
    print(f"Error inserting data into aggregated_user: {e}")

try:
    for index, row in map_insurance_df.iterrows():
        map_insurance_query = '''
        INSERT INTO map_insurance (States, Years, Quarter, Districts, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['States'],
            row['Years'],
            row['Quarter'],
            row['Districts'],
            row['Transaction_count'],
            row['Transaction_amount']
        )
        cursor.execute(map_insurance_query, values)
    mydb.commit()
    print("Data inserted into map_insurance successfully.")
except Exception as e:
    print(f"Error inserting data into map_insurance: {e}")

try:
    for index, row in map_transaction_df.iterrows():
        map_transaction_query = '''
        INSERT INTO map_transaction (States, Years, Quarter, Districts, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['States'],
            row['Years'],
            row['Quarter'],
            row['Districts'],
            row['Transaction_count'],
            row['Transaction_amount']
        )
        cursor.execute(map_transaction_query, values)
    mydb.commit()
    print("Data inserted into map_transaction successfully.")
except Exception as e:
    print(f"Error inserting data into map_transaction: {e}")

try:
    for index, row in map_user_df.iterrows():
        map_user_query = '''
        INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row["States"],
            row["Years"],
            row["Quarter"],
            row["Districts"],
            row["RegisteredUser"],
            row["AppOpens"]
        )
        cursor.execute(map_user_query, values)
    mydb.commit()
    print("Data inserted into map_user successfully.")
except Exception as e:
    print(f"Error inserting data into map_user: {e}")

try:
    for index, row in top_insurance_df.iterrows():
        top_insurance_query = '''
        INSERT INTO top_insurance (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row["States"],
            row["Years"],
            row["Quarter"],
            row["Pincodes"],
            row["Transaction_count"],
            row["Transaction_amount"]
        )
        cursor.execute(top_insurance_query, values)
    mydb.commit()
    print("Data inserted into top_insurance successfully.")
except Exception as e:
    print(f"Error inserting data into top_insurance: {e}")

try:
    for index, row in top_transaction_df.iterrows():
        top_transaction_query = '''
        INSERT INTO top_transaction (States, Years, Quarter, Pincodes, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row["States"],
            row["Years"],
            row["Quarter"],
            row["Pincodes"],
            row["Transaction_count"],
            row["Transaction_amount"]
        )
        cursor.execute(top_transaction_query, values)
    mydb.commit()
    print("Data inserted into top_transaction successfully.")
except Exception as e:
    print(f"Error inserting data into top_transaction: {e}")

try:
    for index, row in top_user_df.iterrows():
        top_user_query = '''
        INSERT INTO top_user (States, Years, Quarter, Pincodes, RegisteredUser)
        VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            row["States"],
            row["Years"],
            row["Quarter"],
            row["Pincodes"],
            row["RegisteredUser"]
        )
        cursor.execute(top_user_query, values)
    mydb.commit()
    print("Data inserted into top_user successfully.")
except Exception as e:
    print(f"Error inserting data into top_user: {e}")

# Close the cursor and connection
cursor.close()
mydb.close()