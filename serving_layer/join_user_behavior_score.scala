// hbase
create 'xqoasis_user_behavior_count_score', 'info'
// hive
create external table xqoasis_user_behavior_count_score (
  user_id string,
  pv bigint,
  fav bigint,
  cart bigint,
  buy bigint,
  R int,
  R_rank int,
  R_score int,
  F int,
  F_rank int,
  F_score int,
  score int
  )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,info:pv#b,info:fav#b,info:cart#b,info:buy#b,info:R,info:R_rank,info:R_score,info:F,info:F_rank,info:F_score,info:score')
TBLPROPERTIES ('hbase.table.name' = 'xqoasis_user_behavior_count_score');

insert overwrite table xqoasis_user_behavior_count_score
  select b.user_id as user_id, b.pv as pv, b.fav as fav, b.cart as cart, b.buy as buy,
  c.R as R, c.R_rank as R_rank, c.R_score as R_score, 
  c.F as F, c.F_rank as F_rank, c.F_score as F_score, c.score as score
  from xqoasis_user_behavior_count b join xqoasis_user_score_merge c
    on b.user_id = c.user_id;

// check
select * from xqoasis_user_behavior_count_score limit 3;