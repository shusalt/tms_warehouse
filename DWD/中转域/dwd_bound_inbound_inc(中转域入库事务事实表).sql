drop table if exists dwd_bound_inbound_inc;
create external table dwd_bound_inbound_inc(
	`id` bigint COMMENT '中转记录ID',
	`order_id` bigint COMMENT '运单ID',
	`org_id` bigint COMMENT '机构ID',
	`inbound_time` string COMMENT '入库时间',
	`inbound_emp_id` bigint COMMENT '入库人员'
) comment '中转域入库事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_inbound_inc'
tblproperties ('orc.compression' = 'snappy');


-- 数据装载
-- 首日数据装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_bound_inbound_inc partition (dt)
select
	data.id,
	data.order_id,
	data.org_id,
	data.inbound_time,
	data.inbound_emp_id,
	date_format(data.inbound_time, 'yyyy-MM-dd') dt
from ods_order_org_bound_inc
where dt = '2024-01-07'
	and data.is_deleted = '0'
	and type = 'bootstrap-insert';




-- 每日装载
insert overwrite table dwd_bound_inbound_inc partition (dt='2024-01-08')
select
	data.id,
	data.order_id,
	data.org_id,
	data.inbound_time,
	data.inbound_emp_id
from ods_order_org_bound_inc
where dt = '2024-01-08' 
	and type = 'insert'
	and data.is_deleted = '0'