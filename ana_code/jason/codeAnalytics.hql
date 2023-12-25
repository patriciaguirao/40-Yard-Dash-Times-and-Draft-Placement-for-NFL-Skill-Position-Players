Hive Queries to Run Analytics Code
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

— Create External Table for my Partners Dataset:
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

— Combine both datasets into 1 table:
CREATE TABLE combined_data AS
SELECT * FROM nfl_pat
UNION ALL
SELECT * FROM nfl_jason;

— Creating table to store our analytics
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

Export Analytics Output to HDFS:
INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/finalProject/ana_code/combineAnalytics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM round_position_analysis;

Export Combined Table to HDFS:
INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/ht2354_nyu_edu/finalProject/ana_code/combinedNFLCombineDataTable'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM combined_data;