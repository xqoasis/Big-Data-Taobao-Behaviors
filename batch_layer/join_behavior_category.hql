joincreate table delays (
    user_id string,
    item_id string,
    category_id string,
    category_desc string,
    behavior_type string,
    behavior_timestamp int,
    behavior_date string)
  stored as orc;

insert overwrite table xqoasis_category
  select b.user_id as user_id, b.item_id as item_id, b.category_id as category_id,
  c.name as category_desc, b.behavior_type as behavior_type,
  b.behavior_timestamp as behavior_timestamp, b.behavior_date as behavior_date
  from xqoasis_user_behavior b join xqoasis_category c
    on b.category_id = c.code;
