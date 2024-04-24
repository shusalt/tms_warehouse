drop table if exists ads_city_stats;
create external table ads_city_stats(
	`dt` string COMMENT '统计日期',
	`recent_days` bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
	`city_id` bigint COMMENT '城市ID',
	`city_name` string COMMENT '城市名称',
	`order_count` bigint COMMENT '下单数',
	`order_amount` decimal COMMENT '下单金额',
	`trans_finish_count` bigint COMMENT '完成运输次数',
	`trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
	`trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
	`avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
	`avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
) comment '城市分析'
row format delimited fields terminated by '\t'
location '/warehouse/tms/ads/ads_city_stats';








-- 数据装载
with city_order_stats as (
	-- 城市下单统计指标
	select
		'2024-01-08' dt,
		recent_days,
		city_id,
		city_name,
		order_count,
		order_amount
	from (
		select
			1 recent_days,
			city_id,
			case when city_id = 110100 then '北京市'
				when city_id = 120100 then '天津市'
				when city_id = 310100 then '上海市'
				when city_id = 500100 then '重庆市'
				else city_name end city_name,
			sum(order_count) order_count,
			sum(order_amount) order_amount
		from dws_trade_org_cargo_type_order_1d
		where dt = '2024-01-08'
		group by city_id, city_name
		union
		select
			recent_days,
			city_id,
			case when city_id = 110100 then '北京市'
				when city_id = 120100 then '天津市'
				when city_id = 310100 then '上海市'
				when city_id = 500100 then '重庆市'
				else city_name end city_name,
			sum(order_count) order_count,
			sum(order_amount) order_amount
		from dws_trade_org_cargo_type_order_nd
		where dt = '2024-01-08'
		group by recent_days, city_id, city_name
	) tmp_tb1		
),
city_trans_stats as (
	-- 城市运输指标统计
	select
		'2024-01-08' dt,
		recent_days,
		city_id,
		city_name,
		trans_finish_count,
		trans_finish_distance,
		trans_finish_dur_sec,
		avg_trans_finish_distance,
		avg_trans_finish_dur_sec
	from (
		select
			1 recent_days,
			city_id,
			city_name,
			sum(trans_finish_count) trans_finish_count,
			sum(trans_finish_distance) trans_finish_distance,
			sum(trans_finish_dur_sec) trans_finish_dur_sec,
			round(sum(trans_finish_distance) / sum(trans_finish_count), 2) avg_trans_finish_distance,
			round(sum(trans_finish_dur_sec) / sum(trans_finish_count)) avg_trans_finish_dur_sec
		from (
			select
				if(dim_organ.org_level=1, dim_city.id, dim_province.id) city_id,
				if(dim_organ.org_level=1, dim_city.name, dim_province.name) city_name,
				trans_finish_count,
				trans_finish_distance,
				trans_finish_dur_sec
			from (
				select
					org_id,
					sum(trans_finish_count) trans_finish_count,
					sum(trans_finish_distance) trans_finish_distance,
					sum(trans_finish_dur_sec) trans_finish_dur_sec
				from dws_trans_org_truck_model_type_trans_finish_1d
				where dt = '2024-01-08'
				group by org_id
			) city_trans_1d_stats1
			left join (
				select
					id,
					org_level,
					region_id
				from dim_organ_full
				where dt = '2024-01-07'
			) dim_organ
			on city_trans_1d_stats1.org_id = dim_organ.id
			left join (
				select
					id,
					case when id = 110100 then '北京市'
						when id = 120100 then '天津市'
						when id = 310100 then '上海市'
						when id = 500100 then '重庆市'
						else name end name,
					parent_id
				from dim_region_full
				where dt = '2024-01-07'
			) dim_city
			on dim_organ.region_id = dim_city.id
			left join (
				select
					id,
					case when id = 110100 then '北京市'
						when id = 120100 then '天津市'
						when id = 310100 then '上海市'
						when id = 500100 then '重庆市'
						else name end name
				from dim_region_full
				where dt = '2024-01-07'
			) dim_province
			on dim_city.parent_id = dim_province.id
		) city_trans_stats2
		group by
			city_id,
			city_name
		union
		select
			recent_days,
			city_id,
			case when city_id = 110100 then '北京市'
				when city_id = 120100 then '天津市'
				when city_id = 310100 then '上海市'
				when city_id = 500100 then '重庆市'
				else city_name end city_name,
			sum(trans_finish_count) trans_finish_count,
			sum(trans_finish_distance) trans_finish_distance,
			sum(trans_finish_dur_sec) trans_finish_dur_sec,
			round(sum(trans_finish_distance) / sum(trans_finish_count), 2) avg_trans_finish_distance,
			round(sum(trans_finish_dur_sec) / sum(trans_finish_count)) avg_trans_finish_dur_sec
		from dws_trans_shift_trans_finish_nd
		where dt = '2024-01-08'
		group by
			recent_days,
			city_id,
			city_name	
	) tmp_tb2
)
insert overwrite table ads_city_stats
select
	dt,
	recent_days,
	city_id,
	city_name,
	order_count,
	order_amount,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	avg_trans_finish_distance,
	avg_trans_finish_dur_sec
from ads_city_stats
union
select
	nvl(city_order_stats.dt, city_trans_stats.dt) dt,
	nvl(city_order_stats.recent_days, city_trans_stats.recent_days)recent_days,
	nvl(city_order_stats.city_id, city_trans_stats.city_id) city_id,
	nvl(city_order_stats.city_name, city_trans_stats.city_name) city_name,
	order_count,
	order_amount,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	avg_trans_finish_distance,
	avg_trans_finish_dur_sec
from city_order_stats
full outer join city_trans_stats
on city_order_stats.dt = city_trans_stats.dt
	and city_order_stats.recent_days = city_trans_stats.recent_days
	and city_order_stats.city_id = city_trans_stats.city_id
	and city_order_stats.city_name = city_trans_stats.city_name;