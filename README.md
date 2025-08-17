#Assumptions
1. Since the KPI's requested are on yearly basis, hence considered data has to be processed in batch, means "BATCH PROCESSING" has to be done
2. Data model adopted is "Star Schema", where "ORDERS" is Fact Table & "CUSTOMER","PRODUCTS" are dimension tables
3. Files land on Azure cloud. Hence created external location in Databricks mounted

#Approach Followed:
1. Followed Medallion Architecture-> Bronze, Silver, Gold
2. Bronze: In this layer I have ingested raw data from files to bronze tables.Below cleansing is performed in bronze layer
  a) Cleansing of column names. Removing white spaces in column names
3. Silver: The cleansing of data is done here.
  a) CUSTOMER: Customer_ID: being a "PRIMARY KEY", should not be null & empty and it should not have duplicates
              Customer_Name: The customer name cannot have special characters and those are not valid names. Hence filtering those records
  b) Products: Product_ID: being a "PRIMARY KEY", should not be null & empty and it should not have duplicates
  c) Orders: Order_ID: being a "PRIMARY KEY", should not be null & empty and it should not have duplicates
            Order_Date: Date is present in multiple formats. Hence formatted the date in single format to have consistency
4. Gold: Stored Aggregated data in gold table
5. KPI Generation: Used SQL queries to generate various KPI's

#Componets Created:
1. utils.py: Contains all the generic fuctions
2. data_ingestion.py: All the functions are defined here
3. data_ingest.ipynb: Notebook where all the functions defined in data_ingestion.py are called and bronze tables are loaded
4. data_cleansing.py: All the cleansing functions are defined here
5. data_cleanse.ipynb: Notebook where all the functions defined in data_cleansing.py are called and silver tables are loaded
6. data_processing.py: All the processing functions are defined here
7. data_processing.ipynb:Notebook where all the functions defined in data_processing.py are called and gold tables are loaded 
8. data_analytics.ipynb: All the KPI's are generated
9. test_ingestion.py: all the test ingestion functions are defined here
10. test_ingest.ipynb: Test cases are called in notebook
11. test_cleansing.py: All the test cases for cleansing are defined here
12. test_cleanse.ipynb: all the test cases for cleansing are executed in this notebook
13. test_processing.py: All the test cases for processing are defined here
14. test_processing.ipynb: All the test case for processing are executed in this notebook
14. config.json: Main config file which drives the execution
15. rules_config.json: Contains cleansing rules for each table & columns
16: schemas.py: Contains pre-defined schema

#Common Utilities
1. utils.py: Created common utility, where all the generic functions are placed and further imported in notebooks

#Schema Evolution
1. Implemented Schema Evolution to handle below scenarios:
  a) If any extra column comes in source when compared with actual schema
  b) If we have received less columns in sources whenc compared with actual schema

#Config File Driven Approach
1. Developed "config.json" file-> It drives the execution as all the details related to which file has to be processed
2. Developed "rules_config.json" file-> this drives the rules for cleansing operation, again making it dynamic

#Optimization
1. Used broadcast join on small datasets customer & products. Since these are dimension tables and have less data
2. Used cache to store the data in memory and used for repeated datasets, which were getting used further
3. Used OPTIMIZE <table_name> ZORDER BY(<column name>) to sort & reduce the partitions and improve the reading process
4. Reduced the number of shuffle partitions by changing spark configuration

#TDD
1. Created compreshensive test cases for each section with all possible scenarios. Used Pytest. 
Note: In databricks best practices, in order to use pytest in our notebook. We have to follow below approach:
a) Define all the functions in <>.py file. Then create a notebook where all the functions from <>.py file will be imported and called
b) Same way test<>.py file has to created where we define our test cases & fixtures, then create a notebook to call that.

#Execution Workflow
1. Created job to execute all the notebooks.
<Data Ingestion>-><Test Data Ingestion>-><Data Cleansing>-><Test Data Cleansing>-><Data Processing>-><Test Data Processing>-><Data Analytics>
