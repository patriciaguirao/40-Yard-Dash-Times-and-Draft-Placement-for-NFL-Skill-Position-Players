1. Create all the necessary directories as specified in the Final Project directions.

2. Download all datasets from PFF - Website for Data sources.

2. Upload all raw datasets into Dataproc. 

3. Make directory for each year of combine dataset and put data sets in there:

4. Use Hive to create external tables for each and then combine all 5 files into 1, afterwards exporting it to hdfs directory to run code cleaning. When exporting table from Hive it may split it into two so it is neccessary to combine the 2 output files into 1 to get allcombinedata.csv 

5. Afterwards run cleanCode.scala to clean my dataset. This code will clean allcombineData.csv by getting rid of all columns except Player,Pos,40yd and create a new column called round which gets the round value for each player in the data by parsing through the original Drafted(tm/rnd/year) column. It will also only keep Players that have the Position: QB, WR, TE, RB and also Players with a 40yd dash time. The code will write this to a new csv file which will be called cleanedCombineData.csv. This new file is the finalized and cleaned Dataset that will then be combined with my partners to run the analytic code. 
 

6. For the data analysis, I first put my cleaned dataset into finalProject/ana_code/jason and my partners into finalProject/ana_code/patricia. Then I connect into Hive run all the queries in code_analytics. Which creates a table that ingests the data in the NFLClean.csv (Patricia's dataset), creates another table that ingests the data in the cleanedCombineData.csv dataset (My dataset), combines the two into a new combined dataset (and exports it to HDFS), creates another table to store our analytics, and then fills that table with our desired analytics (which is the number of players, mean 40-yard time, standard deviation 40-yard time, minimum 40-yard time, maximum 40-yard time, and number of missing 40-yard times for each type of position within each round). The results are then displayed and exported to HDFS (ana_code directory). The combined data is also exported to HDFS.


STEP BY STEP:
— Download all datasets from PFF

— Upload datasets to Dataproc

— Make directory for each year of combine dataset and put data sets in there:
hdfs dfs -mkdir 2019Combine // hdfs dfs -put 2019Combine.csv 2019Combine
hdfs dfs -mkdir 2020Combine // hdfs dfs -put 2020Combine.csv 2020Combine
hdfs dfs -mkdir 2021Combine // hdfs dfs -put 2021Combine.csv 2021Combine
hdfs dfs -mkdir 2022Combine // hdfs dfs -put 2022Combine.csv 2022Combine
hdfs dfs -mkdir 2023Combine // hdfs dfs -put 2023Combine.csv 2023Combine

— Connect to Hive:
beeline -u jdbc:hive2://localhost:10000

— Use my database:
Use ht2354_nyu_edu;

— Create External Table for each Combine Year
CREATE EXTERNAL TABLE IF NOT EXISTS combine_2019 (
    Player STRING,
    Pos STRING,
    School STRING,
    College STRING,
    Ht STRING,
    Wt STRING,
    `40yd` STRING,
    Vertical STRING,
    Bench STRING,
    `Broad Jump` STRING,
    `3Cone` STRING,
    Shuttle STRING,
    `Drafted (tm/rnd/yr)` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/2019Combine';

-- Repeat the above step for 2020Combine, 2021Combine, 2022Combine, and 2023Combine,
-- making sure to change the table name and location.
 — Ingest all 5 tables into 1 table: CREATE TABLE allCombineData AS
SELECT * FROM combine_2019
UNION ALL
SELECT * FROM combine_2020
UNION ALL
SELECT * FROM combine_2021
UNION ALL
SELECT * FROM combine_2022
UNION ALL
SELECT * FROM combine_2023;

— Export allCombineData table to HDFS  INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/finalProject/data_ingest/jason/allcombinedata'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM allcombinedata;

— Hive split the table into two files when exporting so needed to combine into 1 csv file: hdfs dfs -cat finalProject/data_ingest/jason/allcombinedata/000000_0 finalProject/data_ingest/jason/allcombinedata/000001_0 | hdfs dfs -put - finalProject/data_ingest/jason/allcombinedata/allcombinedata.csv

— Remove the 2 separate files since we have allcombinedata.csv now:
hdfs dfs -rm finalProject/data_ingest/jason/allcombinedata/000000_0
hdfs dfs -rm finalProject/data_ingest/jason/allcombinedata/000001_0 

— Copy allcombinedata.csv to local to prepare for code cleaning:
hdfs dfs -get /user/ht2354_nyu_edu/finalProject/data_ingest/jason/allcombinedata/allcombinedata.csv /home/ht2354_nyu_edu/allcombinedata.csv

— Run Cleaning Code (after uploading it to Dataproc):
spark-shell --deploy-mode client -i  cleanCode.scala

— Jason Final Cleaned Dataset after running cleaning code:
cleanedCombineData.csv

— Put Cleaned Datasets (partners + mine) into my hdfs directory under ana_code/jason and ana_code/patricia to prepare to run analytic code

— Connect to Hive:
beeline -u jdbc:hive2://localhost:10000

— Use my database:
Use ht2354_nyu_edu;

— Create External Table for my cleaned Dataset:
CREATE EXTERNAL TABLE nfl_jason (
    player STRING,
    pos STRING,
    40yd DOUBLE,
    round STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/finalProject/ana_code/jason'
TBLPROPERTIES ('skip.header.line.count'='1');

— Create External Table for Partners Data:
CREATE EXTERNAL TABLE nfl_pat (
    player STRING,
    pos STRING,
    40yd DOUBLE,
    round STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/finalProject/ana_code/patricia'
TBLPROPERTIES ('skip.header.line.count'='1');

— Combining both datasets into 1:
CREATE TABLE combined_data AS
SELECT * FROM nfl_pat
UNION ALL
SELECT * FROM nfl_jason;

— Creating table to store analytics
CREATE TABLE round_position_analysis (
  round STRING,
  pos STRING,
  players INT,
  mean_40yd DOUBLE,
  stddev_40yd DOUBLE,
  min_40yd DOUBLE,
  max_40yd DOUBLE,
  missing_values_40yd INT
);

— Run analysis code:
INSERT INTO TABLE round_position_analysis
SELECT
  round,
  pos,
  COUNT(*) AS players,
  AVG(40yd) AS mean_40yd,
  STDDEV(40yd) AS stddev_40yd,
  MIN(40yd) AS min_40yd,
  MAX(40yd) AS max_40yd,
  COUNT(CASE WHEN 40yd IS NULL THEN 1 END) AS missing_values_40yd
FROM combined_data
GROUP BY round, pos;

— Check to see analytic output:
SELECT * FROM round_position_analysis;

Export to Analytics to HDFS:
INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/finalProject/ana_code/combineAnalytics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM round_position_analysis;

Export to Combined Data to HDFS:
INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/finalProject/ana_code/combinedNFLCombineDataTable'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM combined_data;

