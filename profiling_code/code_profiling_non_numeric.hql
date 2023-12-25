-- Going into database
USE ppg2023_nyu_edu;

-- Creating initial table where we will read profiling data into
CREATE EXTERNAL TABLE nfl_profiling (
  school STRING,
  player_type STRING,
  position_type STRING,
  position STRING,
  drafted STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/profiling_code/patricia'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Loading the data in
LOAD DATA INPATH 'hdfs://nyu-dataproc-m/user/ppg2023_nyu_edu/final/data_ingest/patricia/NFlProfilingNonNumeric.csv' INTO TABLE nfl_profiling;

-- Checking if data looks correct
SELECT * FROM nfl_profiling LIMIT 10;

-- Getting unique values for non-numerical columns
SELECT DISTINCT school FROM nfl_profiling;

SELECT DISTINCT player_type FROM nfl_profiling;

SELECT DISTINCT position_type FROM nfl_profiling;

SELECT DISTINCT position FROM nfl_profiling;

SELECT DISTINCT drafted FROM nfl_profiling;