# Snowflake_Schemawide_Smart_Automated_Bulk_Ingestion_Tool
## Friendly tool for non tech savy guys as all the configurations are done in excel, the python code executes in backend but the entire thing can be run using excel

**Description:**
This Python project is designed to automate the ingestion process of CSV files into Snowflake tables across various schemas by push of a single button. The script utilizes the `snowflake.connector` library to establish a connection with Snowflake and the `pandas` library to handle data manipulation and analysis.

**Features:**
1. **Dynamic Configuration:** The project reads a configuration file (`config.json`) and a template (`Configuration-Template.xlsx`) to dynamically configure the Snowflake connection parameters and table-specific settings.

2. **Snowflake Table Interaction:**
   - Connects to Snowflake, selects the appropriate role, warehouse, and schema.
   - Reads the column structure of the target Snowflake table to align with the incoming CSV file.

3. **CSV File Handling:**
   - Detects the encoding of the CSV file and reads the **header row automatically**.
   - Capitalizes all columns and removes the last row if it contains grand total information.
   - Checks for additional columns in the CSV file compared to the Snowflake table and handles them accordingly **(Ignores or Adds to snowflake table if you set the option as Yes)**.

4. **Hashkey Generation:**
   - Creates a hashkey based on specified columns to uniquely identify records.
   - Performs a rip-and-replace operation in Snowflake to delete existing records with matching hashkeys before appending new data **To ensure no duplication of data**.

5. **Data Ingestion:**
   - Prepares the final dataset by concatenating the existing Snowflake table and the cleaned CSV file.
   - Appends the data to the Snowflake table using the `pandas` `to_sql` method.
   - Even if the column placement of the input file changes, **while writing the correct columns will be stacked correctly in snowflake table**.

6. **Archiving:**
   - Archives successfully ingested CSV files to an "Output Folder (Archive)" with a timestamped subfolder.
   - Makes it easy to identify on a specific date in each schame and table. which all files were ingested and which were not!
   - Files after run in input folde rmeans not ingested, In acrhived folder means they were ingested.

7. **Logging:**
   - Prints informative messages throughout the execution to track progress and any encountered issues.

**Note:** This project is actively under development, and future features may include:
- Support for Excels as well.
- Pushing this code as a fully automated lambda fucntion in AWS S3 so files are ingested as soon as they are dropped (event driven pipeline).
- Pushing this code to Glue (event driven pipeline based on Pyspark distributed computing to handle bigger loads as well).
- Ability to auto detect new column types and add the columns with autodetected type in snowflake table when "ADDITIONAL_COLUMNS_SHOULD_BE_ADDED_TO_SF_TABLE"         argument in the `Configuration-Template.xlsx` is set to : "Yes"

**Instructions for Use:**
1. Place CSV files in the "Input Files" folder with subfolders named after the corresponding Snowflake table.
2. Update the `config.json` file with Snowflake credentials.
3. Configure the `Configuration-Template.xlsx` file with the desired settings for each Snowflake table.
   Arguments:
     - OUTPUT_TABLE_NAME - The name of the table in which dta in snowflake has to be ingested. Input folder hsould also have a folder with the same name, All files         inside the folder will be ingested in table one by one.
     - HASHKEY_COLUMNS: Columns speerated by comma based on which the RIP and replace of data will happen to prevent duplication. eg Date, USer_ID will make sure           all unique entries of Date + user_id (which are present in csv file) will be deleted iform snowflake table if they already exist and replaced.
     - ADDITIONAL_COLUMNS_SHOULD_BE_ADDED_TO_SF_TABLE: "No" means do nothing, "Yes" means run an alter table clause and add all the columns present in csv file but         not present in snowflake table to the table.
     - DATA_TYPES_OF_ADDITIONAL_COLUMNS: self explanatory.
     - CSV_HAS_GRANDTOTAL_ROWS: "No" means do nothing, "yes" means delete last row
     - ACCOUNT: Your snowflake Account. (ending in .com)
     - ROLE: The snowflake role you want to use.
     - DATABASE: The snowflake database you want to use.
     - SCHEMA: The snowflake schema you want to use.
     - WAREHOUSE: The snowflake warehouse you want to use.
4. Run the script to automate the ingestion process.

**Disclaimer:** The script assumes a standardized CSV format. Use the provided configuration template only for setting up things!
