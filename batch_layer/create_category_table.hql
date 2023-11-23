-- Make sure you have put category.txt in hdfs:temp/xqoasis/category
DROP TABLE IF EXISTS xqoasis_category;
CREATE EXTERNAL TABLE xqoasis_category (code STRING, name STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location 'temp/xqoasis/category';

select * from xqoasis_category limit 5;