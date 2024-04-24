drop table if exists dwd_bound_sort_inc;
create external table dwd_bound_sort_inc(
	`id` bigint COMMENT '中转记录ID',
	`order_id` bigint COMMENT '订单ID',
	`org_id` bigint COMMENT '机构ID',
	`sort_time` string COMMENT '分拣时间',
	`sorter_emp_id` bigint COMMENT '分拣人员'
) comment '中转域分拣事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_sort_inc'
tblproperties ('orc.compression' = 'snappy');



-- 数据装载
-- 首日数据装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_bound_sort_inc partition (dt)
select
	data.id,
	data.order_id,
	data.org_id,
	data.sort_time,
	data.sorter_emp_id,
	date_format(data.sort_time, 'yyyy-MM-dd') dt
from ods_order_org_bound_inc
where dt = '2024-01-07' 
	and data.is_deleted = '0'
	and type = 'bootstrap-insert'
	and data.sort_time is not null;


-- 每日装载
insert overwrite table dwd_bound_sort_inc partition (dt='2024-01-08')
select
	data.id,
	data.order_id,
	data.org_id,
	data.sort_time,
	data.sorter_emp_id
from ods_order_org_bound_inc
lateral view explode(map_keys(old)) explode_old_key as old_key
where old_key = 'sort_time'	
	and dt = '2024-01-08' 
	and type = 'update'
	and data.is_deleted = '0'
	and data.sorter_emp_id is not null
	and old['sorter_emp_id'] is null


-- 这种方式也可以
-- select
-- 	data.id,
-- 	data.order_id,
-- 	data.org_id,
-- 	data.sort_time,
-- 	data.sorter_emp_id,
-- 	data.sort_time,
-- 	dt,
-- 	old,
-- 	data.status
-- from ods_order_org_bound_inc	
-- where dt = '2024-01-08' 
-- 	and type = 'update'
-- 	and data.is_deleted = '0'
-- 	and data.status = '64003'
