-- Going into database
USE ppg2023_nyu_edu;

-- Creating initial table (mine) where we will read the cleaned data into
CREATE EXTERNAL TABLE nfl_pat (
  player STRING,
  pos STRING,
  40yd DOUBLE,
  round STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/ana_code/patricia/nfl_pat'
TBLPROPERTIES ('skip.header.line.count'='1'); 

-- Loading the data in
LOAD DATA INPATH 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/data_ingest/patricia/NFLClean.csv' INTO TABLE nfl_pat;

-- Checking if data looks correct
SELECT * FROM nfl_pat LIMIT 10;

-- Get number of rows of my cleaned data
SELECT COUNT(*) FROM nfl_pat;

-- Creating initial table (partner's) where we will read the cleaned data into
CREATE EXTERNAL TABLE nfl_jason (
  player STRING,
  pos STRING,
  40yd DOUBLE,
  round STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/ana_code/patricia/nfl_jason'
TBLPROPERTIES ('skip.header.line.count'='1'); 

-- Loading the data in
LOAD DATA INPATH 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/data_ingest/patricia/cleanedCombineData.csv' INTO TABLE nfl_jason;

-- Checking if data looks correct
SELECT * FROM nfl_jason LIMIT 10;

-- Get number of rows of my partner's data
SELECT COUNT(*) FROM nfl_jason;

-- Combining mine and my partner's data
CREATE TABLE combined_data AS
SELECT * FROM nfl_pat
UNION ALL
SELECT * FROM nfl_jason;

-- Get number of rows of combined data
SELECT COUNT(*) FROM combined_data;

-- Creating a table to store our analytics
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

-- Finding the number of players, mean 40-yard time, standard deviation 40-yard time, minimum 40-yard time, maximum 40-yard time, and number of missing 40-yard time values for each round and position
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

SELECT * FROM round_position_analysis;

-- Exporting data to HDFS
INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/ana_code/patricia/nfl_analytics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM round_position_analysis;

-- Exporting data to HDFS
INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/etl_code/patricia/nfl_combined'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM combined_data;