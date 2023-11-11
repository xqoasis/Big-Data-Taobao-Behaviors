-- This file will create an ORC table with Taobao User Behavior data
drop table if exists xqoasis_user_behavior_csv

-- First, map the CSV data we downloaded in Hive
create external table xqoasis_user_behavior_csv(
    user_id string,
    item_id string,
    category_id string,
    behavior_type string,
    behavior_time timestamp,
    behavior_data timestamp)
    row format serde 'oeg.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/xqoasis/final/userbehavior';

-- Run a test query to make sure the above worked correctly
select user_id,item_id,category_id,behavior_type,behavior_time,behavior_data from xqoasis_user_behavior_csv limit 5;

-- Create an ORC table for ontime data (Note "stored as ORC" at the end)
create external table ontime_csv(
    user_id string,
    item_id string,
    category_id string,
    behavior_type string,
    behavior_time timestamp,
    behavior_data timestamp)
    stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table ontime select * from xqoasis_user_behavior_csv
where origin is not null and dest is not null
and depdelay is not null and arrdelay is not null;