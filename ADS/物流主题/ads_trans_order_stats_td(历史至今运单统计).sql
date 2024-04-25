drop table if exists ads_trans_order_stats_td;
create external table ads_trans_order_td_stats(
	`dt` string COMMENT '统计日期',
	`bounding_order_count` bigint COMMENT '运输中运单总数',
	`bounding_order_amount` decimal(16,2) COMMENT '运输中运单金额'
) comment '物流主题历史至今运单统计'
row format delimited fields terminated by '\t'
location '/warehouse/tms/ads/ads_trans_order_stats_td';




-- 数据装载
insert overwrite table ads_trans_order_stats_td
select
	dt,
	bounding_order_count,
	bounding_order_amount
from ads_trans_order_stats_td
union
select
	dispatch_stats.dt,
	(dispatch_order_count - bound_finish_order_count) bounding_order_count,
	(dispatch_order_amount - bound_finish_order_amount)  bounding_order_amount
from (
	select
		'2024-01-08' dt,
		order_count dispatch_order_count,
		order_amount dispatch_order_amount
	from dws_trans_dispatch_td
	where dt = '2024-01-08'	
) dispatch_stats
join (
	select
		'2024-01-08' dt,
		order_count bound_finish_order_count,
		order_amount bound_finish_order_amount
	from dws_trans_bound_finish_td
	where dt = '2024-01-08'
) bound_finish_stats
on dispatch_stats.dt = bound_finish_stats.dt;