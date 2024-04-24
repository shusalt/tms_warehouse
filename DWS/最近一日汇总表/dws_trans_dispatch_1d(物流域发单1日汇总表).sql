drop table if exists dws_trans_dispatch_1d;
create external table dws_trans_dispatch_1d(
	`order_count` bigint comment '发单总数',
	`order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单1日汇总表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_dispatch_1d'
tblproperties ('orc.compression' = 'snappy');


-- 首日装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dws_trans_dispatch_1d partition (dt)
select
	count(order_id) order_count,
	sum(amount) order_amount,
	dt
from (
	select
		order_id,
		max(amount) amount,
		dt
	from dwd_trans_dispatch_detail_inc
	group by
		order_id,
		dt	
) dispatch_info
group by dt;



-- 每日装载
insert overwrite table dws_trans_dispatch_1d partition (dt = '2024-01-08')
select
	count(order_id) order_count,
	sum(amount) order_amount
from (
	select
		order_id,
		max(amount) amount
	from dwd_trans_dispatch_detail_inc
	where dt = '2024-01-08'
	group by
		order_id
) dispatch_info