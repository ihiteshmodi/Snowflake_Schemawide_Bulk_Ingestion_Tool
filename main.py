import pandas as pd
import os, csv
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import psycopg2
import time
import json
import glob
import datetime
import warnings
warnings.filterwarnings('ignore')


class Local_to_snowflake:
    
    def __init__(self,connector_dict, snowflake_table_name, local_file_path, hashkey_columns, output_data_type = "VARCHAR", has_total_on_last_row = "Yes", add_additional_columns = "No"):
        self.connector_dict = connector_dict
        self.snowflake_table_name = snowflake_table_name
        self.local_file_path = local_file_path
        self.autodector_rows_to_read = 20
        self.output_data_type = output_data_type
        self.has_total_on_last_row = has_total_on_last_row
        self.add_additional_columns = add_additional_columns
        self.hashkey_columns = list(hashkey_columns)
        self.capital_hashkey_columns = [x.upper() for x in hashkey_columns]
        self.snowflake_data_types_mapping = {
                                            'object': 'VARCHAR',
                                            'int64': 'INTEGER',
                                            'datetime64[ns]': 'DATETIME',
                                            'float64' : 'NUMBER'
                                            # Add more mappings as needed
                                        }

        

    def connect_to_snowflake(self):
        self.conn = snow.connect(user=self.connector_dict['user'],
        password=self.connector_dict['password'],
        account=self.connector_dict['account'],
        warehouse=self.connector_dict['warehouse'],
        database= self.connector_dict['database'],
        schema= self.connector_dict['schema'])

        #Configuring the engine. its required to push dataframes to snowflakes / any sql servers
        self.engine = create_engine(URL(
            account = self.connector_dict['account'],
            user = self.connector_dict['user'],
            password = self.connector_dict['password'],
            database = self.connector_dict['database'],
            schema = self.connector_dict['schema'],
            warehouse = self.connector_dict['warehouse'],
            role = self.connector_dict['role'],
        ))

        #connecting our engine
        self.connection = self.engine.connect()

        #establishing the connection by connecting our cursor to snowflakes
        self.cur = self.conn.cursor()

        #First, lets conenct and select our role
        self.cur.execute(f"USE ROLE {self.connector_dict['role']}")
        print(f"Connected to Role {self.connector_dict['role']}")

        #then, lets conenct and select our warehouse
        self.cur.execute(f"USE warehouse {self.connector_dict['warehouse']}")
        print(f"Connected to Warehouse  {self.connector_dict['warehouse']}")

        #now, finally selecting our schema in which we want to perform operations
        self.cur.execute(f"USE SCHEMA {self.connector_dict['schema']}")
        print(f"Connected to Schema {self.connector_dict['schema']}")

        print(f"Connection to Role, Warehouse and Schema Complete")
        print(f"Table to read from and write to is: {self.snowflake_table_name}")
        print(" ")

        self.read_snowflake_table_columns()

    def read_snowflake_table_columns(self):
        try:
            self.readtablequery = f'''
                select *
                from {self.snowflake_table_name}
                WHERE 1 = 0
                '''
                #1 = 0 will read only the column names and leave data out

            print(f"Reading only the columns from snowflake table {self.snowflake_table_name}")
            
            self.sftable = pd.read_sql(self.readtablequery, con = self.engine)
            print("Converting all the columns in snowflake table to uppercase")
            self.sftable.columns = [x.upper() for x in self.sftable.columns]
            self.table_not_exists = 0
            print(" ")
            return self
        
        except:
            print(f"Table Not Found, Creating table with name {self.snowflake_table_name} and Ingesting data")
            self.table_not_exists = 1

        finally:
            self.detect_csvfile_encoding()


    def detect_csvfile_encoding(self):
        try:
            with open(self.local_file_path) as f:
                self.encoding = str(f).split("'")[-2]
                print(f"Encoding of the file is : {self.encoding}")
                return self
        except Exception as E:
            print(E)
        finally:
            self.local_file_startrow_autodetector()


    def local_file_startrow_autodetector(self):      
        try:
            df = pd.read_csv(self.local_file_path, encoding = self.encoding)
            self.localfile_header_row = 0
                #If the header is on row no 1, we dont have to make any changes. This is auto update!
            print("Header found on row no 1 itself!")
            return self
            
        except:
            print(f"We will now detect the header row in the first {self.autodector_rows_to_read} Rows")
            for i in range(self.autodector_rows_to_read):
                row_number = i
                    #as we want this value returned this is header number
                df = pd.read_csv(self.local_file_path, encoding = self.encoding, header=None, usecols=[0], nrows=self.autodector_rows_to_read, skiprows=i, low_memory=False)
                df2 = pd.read_csv(self.local_file_path, encoding = self.encoding, header=None, usecols=[1], nrows=self.autodector_rows_to_read, skiprows=i, low_memory=False)
                if df.shape[0] == df2.dropna().shape[0]: 
                    # We will drop the null values as after dropping null values (column 2 must have null rows that are populated with metadata in row 1) the no of rows will reduce and we will keep on skipping rows until the no fo rows are equal
                    break
            
            self.localfile_header_row = row_number - 2
            print(f"Startrow of the File detected on row number {self.localfile_header_row}")
            print(" ")

            return self
    
        finally:
            self.local_file_reader()
    

    
        """
        We have to subtract 2
        1) -1 for header = None as it
        2) additional -1 As we start from 0 not 1 in all programming languages

        -- If starting row number is actually 6, the code will return as 8 as above 2 cases add + 1 each so we subtract -2 from row number.
        """

    def local_file_reader(self):
        try:
            self.df = pd.read_csv(self.local_file_path, encoding = self.encoding, header = self.localfile_header_row, low_memory = False)
            print("Done reading the file")
            
            return self
        except Exception as E:
            print(E)
        
        finally:
            self.validate_if_localfile_was_read_correctly()


    def validate_if_localfile_was_read_correctly(self):
        """
        self.original_header = int(self.localfile_header_row)       #have to add int else we will end up linking variables than assigning htem differntly!

        if (self.original_header + 5 ) == self.localfile_header_row:
            print("Unable to detect headers, Stopping execution")
            raise Exception

        #   We will also update lcoalfile and this fucntion keeps chekcing for it so will never reach +5. have to figure out some other way
        """
        print("Validating if file was read correctly by checking all the columns in Local File")

        self.localfile_reread_required = False                              #Default assumption is that all looks good

        try:
            if "Unnamed" in self.df.columns[-1]:                            #when rows are detected incorrectly, we will have column names looking like "Unnamed:1,2,3" etc.. Sometimes the first row may have value and rest maybe Unnamed 2,3,4 etc.. Best to take last column and check for validation
                print("Headers Unfortunately not read correctly")
                self.localfile_reread_required = True
                self.localfile_header_row = int(self.localfile_header_row) + 1
            return self                                                                   

        except Exception as E:
            print(E)
            
        finally:
            if self.localfile_reread_required == True:
                print("Trying to redetect the headers")
                print(" ")
                self.local_file_reader()                                    #this is a Recursive code piece.
            else:
                print("Headers were detected Correctly")
                print(" ")
                self.capitalized_localfile_columns_and_remove_grandtotal_row()
            


    def capitalized_localfile_columns_and_remove_grandtotal_row(self):
        try:
            def clean_column_name(column_name):
                # Replace non-alphanumeric characters with underscores
                cleaned_name = ''.join(char if char.isalnum() else '_' for char in column_name)
                # Replace multiple underscores with a single underscore
                cleaned_name = '_'.join(part for part in cleaned_name.split('_') if part)
                # Convert to uppercase
                cleaned_name = cleaned_name.upper()
                return cleaned_name
            
            self.df.columns = [clean_column_name(col) for col in self.df.columns]
            print("Converting all column names to standard names with capital letetrs and underscores in place of spaces and other characters")

            if self.has_total_on_last_row == "Yes":
                print("File has grand total row as mentioned by Master so removing the last row from the dataset")
                print(" ")
                self.df = self.df[:-1]

            return self

        except Exception as E:
            print(E)
        
        finally:
            self.check_for_additional_columns()


    def check_for_additional_columns(self):
        try:
            if self.table_not_exists == 1:
                pass
            else:
                print("Checking if any additional columns are present in ")
                local_file_columns = set(self.df.columns)
                snowflake_table_columns = set(self.sftable.columns)
                self.additional_columns = local_file_columns - snowflake_table_columns
                if len(self.additional_columns) > 0:
                    print(f"{len(self.additional_columns)} Additonal Columns Found")
                    print(self.additional_columns)
                    print(" ")
                else:
                    print("No Additional Columns Found")
                    print(" ")
                return self
        except Exception as E:
            print(E)

        finally:
            self.add_additional_columns_to_sf_table()

    
    def add_additional_columns_to_sf_table(self):
        try:        
            if self.add_additional_columns == "No":
                print(f"add_additional_columns is set to {self.add_additional_columns} so additional columns in the File will not be added to the snowflake table and will be dropped form the datfarame")
                self.df.drop(columns = self.additional_columns, inplace = True)
            
            
            else:
                if len(self.additional_columns) > 0:
                    print("Altering the snowflake table to add additional Columns using the Query :")

                    starting_sql_statement = f"ALTER TABLE IF EXISTS {self.snowflake_table_name} ADD COLUMN"

                    if self.output_data_type == "Auto":
                            # Get column names and data types
                        columns_info = self.df.dtypes.to_dict()
                        # Convert the data types to string representation
                        columns_info = {column: str(dtype) for column, dtype in columns_info.items()}
                        snowflake_column_data_types = {column: self.snowflake_data_types_mapping.get(dtype, dtype) for column, dtype in columns_info.items()}

                        for i in self.additional_columns:
                            starting_sql_statement = starting_sql_statement + f' "{i}" {snowflake_column_data_types[i]},'

                        starting_sql_statement = starting_sql_statement[:-1]   #A comma would have come in the end as well, We will remove it

                    else:
                        for i in self.additional_columns:
                            starting_sql_statement = starting_sql_statement + f' "{i}" {self.output_data_type},'

                        starting_sql_statement = starting_sql_statement[:-1]   #A comma would have come in the end as well, We will remove it

                    print(starting_sql_statement)
                    self.cur.execute(starting_sql_statement)
                    print("")
                    print("Query executed and additional columns added to the Snowflake Table")
                    print("We will now reread the snowflake table columns as addiitonal column has been added to be able to append correctly")

                    self.read_snowflake_table_columns()  #running the fucntion above to get the updated table again.

                    print("Done Reading the new snowflake table column structure")
                    print(" ")

                else:
                    print("As the local csv file does not have additional columns, Not adding anything and skipping this step")
                    print(" ")

                return self
        
        except Exception as E:
            print(E)
        
        finally:
            self.create_hashkey_from_local_file()


    
    #Declaring this piece as legacy, as adding column is causing huge issues but directly running delete would be cool
    def create_hashkey_from_local_file(self):
        try:
            df = self.df.copy()
            df["HASHKEY"] = ""
            for i in self.capital_hashkey_columns:
                df["HASHKEY"] = df["HASHKEY"] + df[i].astype(str) #concatenating all columns as strings to form hashkey
                df["HASHKEY"] = df["HASHKEY"].astype(str)
            
            self.unique_ripandreplace_hashkeys = df["HASHKEY"].unique()
            print(f"All the Unique Hashkeys present in the local csv files are {self.unique_ripandreplace_hashkeys}")
            self.go_ahead = "Y"                                     #Have to assign here else function will break
            return self
        except:
            self.unique_ripandreplace_hashkeys = None
            print(f"Columns that we want to use for Hashkey {self.hashkey_columns} are not present in the local csv file")
            print("If you still want to go ahead and run the ingestion, enter 'Y' and hit enter, else enter 'N'")
            self.go_ahead = input()
            self.go_ahead = str(self.go_ahead)
            

        finally:
            if self.go_ahead == "Y":
                self.perform_rip_and_replace()
            else:
                pass

    def perform_rip_and_replace(self):
        try:
            if self.table_not_exists == 1:
                pass
            else:
                RIP_AND_REPLACE_sql_statement = f"DELETE FROM {self.snowflake_table_name} WHERE CONCAT("
                for i in self.capital_hashkey_columns:
                    RIP_AND_REPLACE_sql_statement = RIP_AND_REPLACE_sql_statement + f'"{i}",'

                RIP_AND_REPLACE_sql_statement = RIP_AND_REPLACE_sql_statement[:-1]           #A comma would have come in the end as well, We will remove it
                RIP_AND_REPLACE_sql_statement = RIP_AND_REPLACE_sql_statement + ") IN ("     #Have to close the bracket as well :)

                for i in self.unique_ripandreplace_hashkeys:
                    RIP_AND_REPLACE_sql_statement = RIP_AND_REPLACE_sql_statement + f"'{i}',"
                
                RIP_AND_REPLACE_sql_statement = RIP_AND_REPLACE_sql_statement[:-1]           #A comma would have come in again so have to remove it
                RIP_AND_REPLACE_sql_statement = RIP_AND_REPLACE_sql_statement + ")"          #Closing the brackets of the "IN" Clause

                print(f"The delete statement that we will be executing to delete currently present records using the unique combinations columns {self.hashkey_columns} perform the RIP AND REPLACE is {RIP_AND_REPLACE_sql_statement}")
                print(" ")
                print(" ")
                self.cur.execute(RIP_AND_REPLACE_sql_statement)

        except Exception as E:
            print(E)

        finally:
            self.prepare_and_ingest_final_dataset()


    def prepare_and_ingest_final_dataset(self):
        #if self.unique_ripandreplace_hashkeys = None:
        #    raise Exception

        print("Creating the final dataset where all columns from the csv file get aligned with Snowflake table before we can append the data to the table")
        print(" ")

        if self.table_not_exists == 1:
            self.ingestion_df =  self.df.copy()
        else:
            self.ingestion_df = pd.concat([self.sftable, self.df])

        mode = "append"  #available options are "append, replace, and fail" replace will replace the entire table. fail is default
        self.ingestion_df.to_sql(self.snowflake_table_name, con = self.engine , index=False, if_exists = mode) # method=pd_writer removng this piece As iske bina nahi chalra tha

        print("Done appending the data to snowflake table, Archiving the file to output Folder")
        print(" ")

        self.cur.close()
        self.connection.close()
        print("Closed the snowflake connections")
        

print(f"Changing curent working directory to the script path")
cwd = os.path.dirname(os.path.abspath(__file__)) #this is better than plain os.getcwd
os.chdir(cwd)
print(f"Current working directory changed to {cwd}")


today = datetime.date.today().strftime("%Y-%m-%d")
archive_folder_path = cwd + "\\" +  "Output Folder (Archive)" + "\\" + today
print(f"Creating an archive folder if it does not already exist here : {archive_folder_path}")
if not os.path.exists(archive_folder_path):
    os.makedirs(archive_folder_path)


print("Reading the snowflake Username and Password from config.json")
with open("config.json") as f:
  credentials_dict = json.load(f)


print("Reading the Configuration Template")
config_df = pd.read_excel("Configuration-Template.xlsx")



for i in range(len(config_df)):
    connector_dict = {
        'account': config_df["ACCOUNT"][i],
        'user': credentials_dict["user"],
        'password': credentials_dict["password"],
        'database': config_df["DATABASE"][i],
        'schema': config_df["SCHEMA"][i],
        'warehouse': config_df["WAREHOUSE"][i],
        'role': config_df["ROLE"][i]
    }

    snowflake_tablename_to_append_data_to = config_df["OUTPUT_TABLE_NAME"][i]
    output_data_type = config_df["DATA_TYPES_OF_ADDITIONAL_COLUMNS"][i]
    does_the_csv_file_have_grandtotal_row = config_df["CSV_HAS_GRANDTOTAL_ROWS"][i]
    add_additional_columns_to_snowflake = config_df["ADDITIONAL_COLUMNS_SHOULD_BE_ADDED_TO_SF_TABLE"][i]
    hashkey_columns_list = list(config_df["HASHKEY_COLUMNS"][i].split(","))

    input_folder_name = cwd + "\\" + "Input Files" + "\\" + config_df["OUTPUT_TABLE_NAME"][i]

    files_list = glob.glob(os.path.join(input_folder_name, "*.csv"))
    

    for i in files_list:
        local_csv_filepath = i

        #Assigning our class and its configs to a variable
        floodlight_data_to_snowflake = Local_to_snowflake(connector_dict = connector_dict,
                                                 snowflake_table_name = snowflake_tablename_to_append_data_to,
                                                 local_file_path = local_csv_filepath, 
                                                 hashkey_columns = hashkey_columns_list,
                                                 output_data_type = output_data_type,
                                                 has_total_on_last_row = does_the_csv_file_have_grandtotal_row,
                                                 add_additional_columns = add_additional_columns_to_snowflake)
        
        try:
            #Executing the code
            floodlight_data_to_snowflake.connect_to_snowflake()

            print(f"Successfully ingested the file named : {i} now moving it to archive folder")
            file_archive_path = archive_folder_path + "\\" + local_csv_filepath.split("\\")[-2] + "\\" +  local_csv_filepath.split("\\")[-1] #-2 will give us table name, -1 will give file name witrh .csv extension
            file_archive_folder = archive_folder_path + "\\" + local_csv_filepath.split("\\")[-2] #-2 will give us table name we have to cretae a folder for files to be moved there!
            if not os.path.exists(file_archive_folder):
                os.makedirs(file_archive_folder)

            os.rename(i, file_archive_path)
            print("Dropped the file in Archive")
        except Exception as E:
            print(f"unable to ingest file {i}, Due to this error: {E}")






