-- Make sure you have put category.txt in hdfs://xqoasis/final/category
CREATE EXTERNAL TABLE category (code STRING, name STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/xqoasis/final/category';