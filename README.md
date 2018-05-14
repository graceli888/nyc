# nyc

# 1. create staging tables to store the original csv files
# 2. create control files to import data of csv files
# Control File: nyc_controlFile.ctl
IMPORT DATA 
INTO TABLE nycdbt.staging_tbl_2016
FROM 'C:\HanaProj\Parking_Violations_Issued_-_Fiscal_Year_2016.csv'
SKIP FIRST 1 ROW
RECORD DELIMITED BY '\n'
FIELD DELIMITED BY ','
NO TYPE CHECK
ERROR LOG C:\HanaProj\Parking_Violations_Issued_-_Fiscal_Year_2016.err
