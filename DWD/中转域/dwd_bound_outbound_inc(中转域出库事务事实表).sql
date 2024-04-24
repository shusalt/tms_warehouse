drop table if exists dwd_bound_outbound_inc;
create external table dwd_bound_outbound_inc(
	`id` bigint COMMENT '中转记录ID',
	`order_id` bigint COMMENT '订单ID',
	`org_id` bigint COMMENT '机构ID',
	`outbound_time` string COMMENT '出库时间',
	`outbound_emp_id` bigint COMMENT '出库人员'
) comment '中转域出库事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_outbound_inc'
tblproperties ('orc.compression' = 'snappy');



-- 数据装载
-- 首日数据装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_bound_outbound_inc partition (dt)
select
	data.id,
	data.order_id,
	data.org_id,
	data.outbound_time,
	data.outbound_emp_id,
	date_format(data.outbound_time, 'yyyy-MM-dd') dt
from ods_order_org_bound_inc
where dt = '2024-01-07' 
	and data.is_deleted = '0'
	and type = 'bootstrap-insert'
	and data.outbound_time is not null;


-- 每日装载
insert overwrite table dwd_bound_outbound_inc partition (dt='2024-01-08')
select
	data.id,
	data.order_id,
	data.org_id,
	data.outbound_time,
	data.outbound_emp_id
from ods_order_org_bound_inc
lateral view explode(map_keys(old)) explode_old_key as old_key
where old_key = 'outbound_time'	
	and dt = '2024-01-08' 
	and type = 'update'
	and data.is_deleted = '0'
	and data.outbound_time is not null
	and old['outbound_time'] is null