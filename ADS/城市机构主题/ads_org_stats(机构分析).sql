drop table if exists ads_org_stats;
create external table ads_org_stats(
	`dt` string COMMENT '统计日期',
	`recent_days` tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
	`org_id` bigint COMMENT '机构ID',
	`org_name` string COMMENT '机构名称',
	`order_count` bigint COMMENT '下单数',
	`order_amount` decimal COMMENT '下单金额',
	`trans_finish_count` bigint COMMENT '完成运输次数',
	`trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
	`trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
	`avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
	`avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
) comment '机构分析'
row format delimited fields terminated by '\t'
location '/warehouse/tms/ads/ads_org_stats';




-- 数据装载
with org_order_stats as (
	-- 机构下单统计指标
	select
		'2024-01-08' dt,
		recent_days,
		org_id,
		org_name,
		order_count,
		order_amount
	from (
		select
			1 recent_days,
			org_id,
			org_name,
			sum(order_count) order_count,
			sum(order_amount) order_amount
		from dws_trade_org_cargo_type_order_1d
		where dt = '2024-01-08'
		group by org_id, org_name
		union
		select
			recent_days,
			org_id,
			org_name,
			sum(order_count) order_count,
			sum(order_amount) order_amount
		from dws_trade_org_cargo_type_order_nd
		where dt = '2024-01-08'
		group by recent_days, org_id, org_name
	) tmp_tb1		
),
org_trans_stats as (
	-- 机构运输指标统计
	select
		'2024-01-08' dt,
		recent_days,
		org_id,
		org_name,
		trans_finish_count,
		trans_finish_distance,
		trans_finish_dur_sec,
		avg_trans_finish_distance,
		avg_trans_finish_dur_sec
	from (
		select
			1 recent_days,
			org_id,
			org_name,
			sum(trans_finish_count) trans_finish_count,
			sum(trans_finish_distance) trans_finish_distance,
			sum(trans_finish_dur_sec) trans_finish_dur_sec,
			round(sum(trans_finish_distance) / sum(trans_finish_count), 2) avg_trans_finish_distance,
			round(sum(trans_finish_dur_sec) / sum(trans_finish_count)) avg_trans_finish_dur_sec
		from dws_trans_org_truck_model_type_trans_finish_1d
		where dt = '2024-01-08'
		group by
			org_id,
			org_name
		union
		select
			recent_days,
			org_id,
			org_name,
			sum(trans_finish_count) trans_finish_count,
			sum(trans_finish_distance) trans_finish_distance,
			sum(trans_finish_dur_sec) trans_finish_dur_sec,
			round(sum(trans_finish_distance) / sum(trans_finish_count), 2) avg_trans_finish_distance,
			round(sum(trans_finish_dur_sec) / sum(trans_finish_count)) avg_trans_finish_dur_sec
		from dws_trans_shift_trans_finish_nd
		where dt = '2024-01-08'
		group by
			recent_days,
			org_id,
			org_name
	) tmp_tb2
)
insert overwrite table ads_org_stats
select
	dt,
	recent_days,
	org_id,
	org_name,
	order_count,
	order_amount,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	avg_trans_finish_distance,
	avg_trans_finish_dur_sec
from ads_org_stats
union
select
    nvl(org_order_stats.dt, org_trans_stats.dt) dt,
    nvl(org_order_stats.recent_days, org_trans_stats.recent_days)recent_days,
    nvl(org_order_stats.org_id, org_trans_stats.org_id) org_id,
    nvl(org_order_stats.org_name, org_trans_stats.org_name) org_name,
	order_count,
	order_amount,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	avg_trans_finish_distance,
	avg_trans_finish_dur_sec
from org_order_stats
full outer join org_trans_stats
on org_order_stats.dt = org_trans_stats.dt
	and org_order_stats.recent_days = org_trans_stats.recent_days
	and org_order_stats.org_id = org_trans_stats.org_id
	and org_order_stats.org_name = org_trans_stats.org_name;