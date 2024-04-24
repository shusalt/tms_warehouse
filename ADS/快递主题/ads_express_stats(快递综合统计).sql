drop table if exists ads_express_stats;
create external table ads_express_stats(
	`dt` string COMMENT '统计日期',
	`recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
	`deliver_suc_count` bigint COMMENT '派送成功次数（订单数）',
	`sort_count` bigint COMMENT '分拣次数'
) comment '快递综合统计'
row format delimited fields terminated by '\t'
location '/warehouse/tms/ads/ads_express_stats';




-- 数据装载
insert overwrite table ads_express_stats
select
	dt,
	recent_days,
	deliver_suc_count,
	sort_count
from ads_express_stats
union
select
	nvl(deliver_stats.dt, sort_stats.dt) dt,
	nvl(deliver_stats.recent_days, sort_stats.recent_days) recent_days,
	order_count deliver_suc_count,
	sort_count
from (
	select
		'2024-01-08' dt,
		recent_days,
		order_count
	from (
		select
			1 recent_days,
			sum(order_count) order_count
		from dws_trans_org_deliver_suc_1d
		where dt = '2024-01-08'
		union
		select
			recent_days,
			sum(order_count) order_count
		from dws_trans_org_deliver_suc_nd
		where dt = '2024-01-08'
		group by
			recent_days	
	) tmp1
) deliver_stats
full outer join (
	select
		'2024-01-08' dt,
		recent_days,
		sort_count
	from (
		select
			1 recent_days,
			sum(sort_count) sort_count
		from dws_trans_org_sort_1d
		where dt = '2024-01-08'
		union
		select
			recent_days,
			sum(sort_count) sort_count
		from dws_trans_org_sort_nd
		where dt = '2024-01-08'
		group by recent_days	
	) tmp2
) sort_stats
on deliver_stats.dt = sort_stats.dt and deliver_stats.recent_days = sort_stats.recent_days;