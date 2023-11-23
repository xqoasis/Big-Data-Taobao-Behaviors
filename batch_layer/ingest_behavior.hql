-- This file will create an csv table with Taobao User Behavior data


-- First, map the CSV data we downloaded in Hive
drop table if exists xqoasis_user_behavior_csv;
create external table xqoasis_user_behavior_csv(
    user_id string,
    item_id string,
    category_id string,
    behavior_type string,
    behavior_timestamp int,
    behavior_date string)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location 'temp/xqoasis/userbehavior';

-- Run a test query to make sure the above worked correctly
select user_id,item_id,category_id,behavior_type,behavior_timestamp,behavior_date from xqoasis_user_behavior_csv limit 5;

-- Create an ORC table (Note "stored as ORC" at the end)
drop table if exists xqoasis_user_behavior;
create table xqoasis_user_behavior(
    user_id string,
    item_id string,
    category_id string,
    behavior_type string,
    behavior_timestamp int,
    behavior_date string)
    stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table xqoasis_user_behavior select * from xqoasis_user_behavior_csv
where user_id is not null and item_id is not null
and category_id is not null and behavior_type is not null
and behavior_timestamp is not null;


-- Check data sample
select * from xqoasis_user_behavior limit 5;

-- Data clean: drop duplicated row
insert overwrite table xqoasis_user_behavior
select user_id, item_id, category_id, behavior_type, behavior_timestamp, behavior_date
from xqoasis_user_behavior
group by user_id, item_id, category_id, behavior_type, behavior_timestamp, behavior_date;

-- Generalize the date type
insert overwrite table xqoasis_user_behavior
select user_id, item_id, category_id, behavior_type, behavior_timestamp, from_unixtime(behavior_timestamp, 'yyyy-MM-dd HH:mm:ss')
from xqoasis_user_behavior;

-- Check any error date
select date(behavior_date) as day from xqoasis_user_behavior group by date(behavior_date) order by day;

-- Drop data outside the time range (they are error value)
insert overwrite table xqoasis_user_behavior
select user_id, item_id, category_id, behavior_type, behavior_timestamp, behavior_date
from xqoasis_user_behavior
where cast(behavior_date as date) between '2017-11-25' and '2017-12-03';

-- check error value of behavior type (they should have only 4 type)
select behavior_type from xqoasis_user_behavior group by behavior_type;


-- drop the csv table
drop table if exists xqoasis_user_behavior_csv;