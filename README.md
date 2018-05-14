

# 1. Create staging tables to store the original csv files
# 2. Create control files to import data of csv files
#    i.e. Control File: nyc_controlFile.ctl
     IMPORT DATA 
     INTO TABLE nycdbt.staging_tbl_2016
     FROM 'C:\HanaProj\Parking_Violations_Issued_-_Fiscal_Year_2016.csv'
     SKIP FIRST 1 ROW
     RECORD DELIMITED BY '\n'
     FIELD DELIMITED BY ','
     NO TYPE CHECK
     ERROR LOG 'C:\HanaProj\Parking_Violations_Issued_-_Fiscal_Year_2016.err'
# 3. Create fact/dimension tables in HCP following data warehouse design(fact_ticket, dim_date, dim_violation, dim_action, dim_community, etc)
# 4. Create stored procedures to implement ETL processing, including dimensions and fact tables. Type two(add a new row) is implemented for slowly changing dimensions
# 5. Perform analytics
#    5.1.  Identify the top 5 most ticketed car brands by year & month
           DECLARE v_year INTEGER;
           DECLARE v_month INTEGER;
           SELECT TOP 5 C.VEHICLE_MAKER, 
                  COUNT(*) AS CNT
             FROM nycdbt.fact_ticket FT INNER JOIN nycdbt.dim_vehicle_type C ON FT.DM_VEHICLE_TYPE_ID = C.DM_VEHICLE_TYPE_ID
                                        INNER JOIN nycdbt.dim_date D         ON FT.ISSUE_DATE_ID = D.DM_DATE_ID
            WHERE D.YEAR = v_year
              AND D.MONTH = v_month
            GROUP BY C.VEHICLE_MAKER
            ORDER BY COUNT(*) DESC
#     5.2   Identify the county with most number of parking infractions?
            SELECT T.COUNTY_NAME, MAX(T.RANK) AS MAX_RANK
              FROM (
                      SELECT C.COUNTY_NAME,
                             RANK() OVER(PARTITION BY C.DM_COUNTY_ID) AS RANK
                        FROM nycdbt.fact_ticket FT INNER JOIN nycdbt.dim_street S  ON FT.DM_STREET_ID = S.DM_STREET_ID
                                                   INNER JOIN nycdbt.dim_county C  ON S.DM_COUNTY_ID = C.DM_COUNTY_ID
                   ) AS T
#     5.3   Street with the most number of parking infraction?
            SELECT T.STREET_NAME, T.COUNTY_NAME, MAX(T.RANK) AS MAX_RANK
              FROM (
                      SELECT S.STREET_NAME, C.COUNTY_NAME, 
                             RANK() OVER(PARTITION BY S.DM_STREET_ID) AS RANK
                        FROM nycdbt.fact_ticket FT  INNER JOIN nycdbt.dim_street S  ON FT.DM_STREET_ID = S.DM_STREET_ID
                                                    INNER JOIN nycdbt.dim_county C  ON S.DM_COUNTY_ID = C.DM_COUNTY_ID
                   ) AS T
#     5.4   Top 100 frequently offending plate numbers
            SELECT TOP 100 P.PLATE_NUMBER, T.CNT
              FROM ( 
                       SELECT FT.DM_PLATE_ID, 
                              COUNT(*) OVER (PARTITION BY FT.DM_PLATE_ID) AS CNT
                         FROM nycdbt.fact_ticket FT
                    ) AS T      INNER JOIN   nycdbt.DIM_PLATE  P   ON T.DM_PLATE_ID = P.DM_PLATE_ID
             ORDER BY T.CNT DESC
              
