drop table if exists dws_trans_bound_finish_td;
create external table dws_trans_bound_finish_td(
	`order_count` bigint comment '发单数',
	`order_amount` decimal(16,2) comment '发单金额'
) comment '物流域转运完成历史至今汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location 'warehouse/tms/dws/dws_trans_bound_finish_td'
tblproperties('orc.compress'='snappy');




-- 数据装载
-- 首日装载
insert overwrite table dws_trans_bound_finish_td partition (dt='2024-01-08')
select
	count(order_id) order_count,
	sum(amount) order_amount
from (
	select
		order_id,
		max(amount) amount
	from dwd_trans_bound_finish_detail_inc
	group by order_id
) bound_info;




-- 每日装载
insert overwrite table dws_trans_bound_finish_td partition (dt='2024-01-08')
select
	sum(order_count) order_count,
	sum(order_amount) order_amount
from (
	select
		order_count,
		order_amount
	from dws_trans_bound_finish_td
	where dt >= date_add('2024-01-08', -1)
	union
	select
		count(order_id) order_count,
		sum(amount) order_amount
	from (
		select
			order_id,
			max(amount) amount
		from dwd_trans_bound_finish_detail_inc
		where dt = '2024-01-08'
		group by order_id
	) bound_info
) final_tb
