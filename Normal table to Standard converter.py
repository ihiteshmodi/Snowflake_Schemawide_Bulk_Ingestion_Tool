#Importing all the packages

import snowflake.connector
import os
import json
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd
import re


print(f"Changing curent working directory to the script path")
cwd = os.path.dirname(os.path.abspath(__file__)) #this is better than plain os.getcwd
os.chdir(cwd)
print(f"Current working directory changed to {cwd}")

print("Reading the snowflake Username and Password from config.json")
with open("config.json") as f:
  credentials_dict = json.load(f)


snowflake_data_types_mapping = {
    'object': 'VARCHAR',
    'int64': 'INTEGER',
    'datetime64[ns]': 'DATETIME',
    'float64' : 'NUMBER'
    # Add more mappings as needed
}


def create_uppercase_table(connection, source_table, destination_table, engine):
    cursor = connection.cursor()

    readtablequery = f'''
                select *
                from {source_table}
                LIMIT 1
                '''
                #1 = 0 will read only the column names and leave data out

    print(f"Reading only the columns from snowflake table {source_table}")
    
    sftable = pd.read_sql(readtablequery, con = engine)

    # Get column names and data types
    columns_info = sftable.dtypes.to_dict()

    # Convert the data types to string representation
    columns_info = {column: str(dtype) for column, dtype in columns_info.items()}

    snowflake_column_data_types = {column: snowflake_data_types_mapping.get(dtype, dtype) for column, dtype in columns_info.items()}

    def clean_key(key):
        # Convert to uppercase and replace non-alphanumeric characters with underscores
        cleaned_key = re.sub(r'[^A-Za-z0-9]+', '_', key.upper())
        return cleaned_key

    cleaned_dict = {clean_key(key): value for key, value in snowflake_column_data_types.items()}

    # create_sql_statement = f"CREATE TABLE IF NOT EXISTS {destination_table} ("
        #Disabling this as we want an error if table already exists, already existing table may not have standard convention!
    create_sql_statement = f"CREATE TABLE {destination_table} ("

    for key, value in cleaned_dict.items():
        create_sql_statement = create_sql_statement + f'"{key}" {value},'

    create_sql_statement = create_sql_statement[:-1] + ")"   #removing last , that would have come and adding a closing bracket
    print("Executing sql query to create standard table")
    try:
        cursor.execute(create_sql_statement)
        print("")
        print(create_sql_statement)

        print("")
        print(f"Inserting into {destination_table} all data from {source_table}")

        insert_into_statement = f"INSERT INTO {destination_table} SELECT * FROM {source_table} LIMIT 5000"
        cursor.execute(insert_into_statement)
        print("")
        print("Insert Into Complete")
        
    except snowflake.connector.errors.ProgrammingError as E:
        print(E)


    cursor.close()


def main():
#Creating our connector dictionary that will be used to create engine fo rpandas as well as cursor for executing SQL statements
    connector_dict = {
            'account': "kinesso.us-east-1",
            'user': credentials_dict["user"],
            'password': credentials_dict["password"],
            'database': "GR_KINESSO",
            'schema': "UM_AMEX_US",
            'warehouse': "UM_AMEX_US",
            'role': "UM_AMEX_US"
        }

    #Engine is for pandas
    engine = create_engine(URL(
                account = connector_dict['account'],
                user = connector_dict['user'],
                password = connector_dict['password'],
                database = connector_dict['database'],
                schema = connector_dict['schema'],
                warehouse = connector_dict['warehouse'],
                role = connector_dict['role'],
            ))
    
    # this Connect if for running SQL queries on snowflake
    connection = snowflake.connector.connect(
        user = connector_dict['user'],
        password = connector_dict['password'],
        account = connector_dict['account'],
        warehouse = connector_dict['warehouse'],
        database = connector_dict['database'],
        schema = connector_dict['schema']
    )

    # Replace these values with your source and destination table names
    source_table = 'DCM_DIM_FACT'
    destination_table = 'DCM_DIM_FACT_STANDARD'

    # Call the function to create the uppercase table
    create_uppercase_table(connection, source_table, destination_table, engine)

    # Close the Snowflake connection
    connection.close()

if __name__ == "__main__":
    main()