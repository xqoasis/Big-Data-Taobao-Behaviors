// User flow and shopping behavior

// each user behavior count
var xqoasis_user_behavior_count = spark.sql("""
select user_id,
       sum(case when behavior_type = 'pv' then 1 else 0 end) as pv,
       sum(case when behavior_type = 'fav' then 1 else 0 end) as fav,
       sum(case when behavior_type = 'cart' then 1 else 0 end) as cart,
       sum(case when behavior_type = 'buy' then 1 else 0 end) as buy
from xqoasis_user_behavior
group by user_id
order by buy desc;""")
xqoasis_user_behavior_count.createOrReplaceTempView("xqoasis_user_behavior_count")

// The top 10 Buy user
xqoasis_user_behavior_count.show().limit(10)

// The totol unique user number
val ubc = spark.table("xqoasis_user_behavior_count")
val total_user = ubc.count()


// hbase
// create 'xqoasis_user_behavior_count', 'behavior'

// hive
create external table xqoasis_user_behavior_count (
  user_id string,
  pv bigint,
  fav bigint,
  cart bigint,
  buy bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,behavior:pv#b,behavior:fav#b,behavior:cart#b,behavior:buy#b')
TBLPROPERTIES ('hbase.table.name' = 'xqoasis_user_behavior_count');




// var total_pv_uv = spark.sql(
//     select sum(case when behavior_type = 'pv' then 1 else 0 end) as pv,
//        count(distinct user_id) as uv
// from user_behavior;)

// PV, UV per day
// select date(datetime) as day,
//        sum(case when behavior_type = 'pv' then 1 else 0 end) as pv,
//        count(distinct user_id) as uv
// from user_behavior
// group by date(datetime)
// order by day;

// User buy again / User count
// select sum(case when buy > 1 then 1 else 0 end) / sum(case when buy > 0 then 1 else 0 end)
// from user_behavior_count;