drop table if exists ads_express_city_stats;
create external table ads_express_city_stats(
	`dt` string COMMENT '统计日期',
	`recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
	`city_id` bigint COMMENT '城市ID',
	`city_name` string COMMENT '城市名称',
	`receive_order_count` bigint COMMENT '揽收次数',
	`receive_order_amount` decimal(16,2) COMMENT '揽收金额',
	`deliver_suc_count` bigint COMMENT '派送成功次数',
	`sort_count` bigint COMMENT '分拣次数'
) comment '各城市快递统计'
row format delimited fields terminated by '\t'
location '/warehouse/tms/ads/ads_express_city_stats';






-- 数据装载
insert overwrite table ads_express_city_stats
select
	dt,
	recent_days,
	city_id,
	city_name,
	receive_order_count,
	receive_order_amount,
	deliver_suc_count,
	sort_count
from ads_express_city_stats
union
select
	nvl(receive_deliver_stats.dt, sort_stats.dt) dt,
	nvl(receive_deliver_stats.recent_days, sort_stats.recent_days) recent_days,
	nvl(receive_deliver_stats.city_id, sort_stats.city_id) city_id,
	nvl(receive_deliver_stats.city_name, sort_stats.city_name) city_name,
	receive_order_count,
	receive_order_amount,
	deliver_suc_count,
	sort_count
from (
	select
		nvl(receive_stats.dt, deliver_stats.dt) dt,
		nvl(receive_stats.recent_days, deliver_stats.recent_days) recent_days,
		nvl(receive_stats.city_id, deliver_stats.city_id) city_id,
		nvl(receive_stats.city_name, deliver_stats.city_name) city_name,
		receive_order_count,
		receive_order_amount,
		deliver_suc_count	
	from (
		-- 揽收
		select
			'2024-01-08' dt,
			recent_days,
			city_id,
			city_name,
			receive_order_count,
			receive_order_amount
		from (
			select
				1 recent_days,
				city_id,
				case when city_id = 110100 then '北京市'
					when city_id = 120100 then '天津市'
					when city_id = 310100 then '上海市'
					when city_id = 500100 then '重庆市'
					else city_name end city_name,
				sum(order_count) receive_order_count,
				sum(order_amount) receive_order_amount
			from dws_trans_org_receive_1d
			where dt = '2024-01-08'
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
				sum(order_count) receive_order_count,
				sum(order_amount) receive_order_amount
			from dws_trans_org_receive_nd
			where dt = '2024-01-08'
			group by
				recent_days,
				city_id,
				city_name	
		) tmp1
	) receive_stats
	full outer join (
		-- 派送成功
		select
			'2024-01-08' dt,
			recent_days,
			city_id,
			city_name,
			deliver_suc_count
		from (
			select
				1 recent_days,
				city_id,
				case when city_id = 110100 then '北京市'
					when city_id = 120100 then '天津市'
					when city_id = 310100 then '上海市'
					when city_id = 500100 then '重庆市'
					else city_name end city_name,
				sum(order_count) deliver_suc_count
			from dws_trans_org_deliver_suc_1d
			where dt = '2024-01-08'
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
				sum(order_count) deliver_suc_count
			from dws_trans_org_deliver_suc_nd
			where dt = '2024-01-08'
			group by
				recent_days,
				city_id,
				city_name	
		) tmp2
	) deliver_stats
	on receive_stats.dt = deliver_stats.dt
		and receive_stats.recent_days = deliver_stats.recent_days
		and receive_stats.city_id = deliver_stats.city_id
		and receive_stats.city_name = deliver_stats.city_name	
) receive_deliver_stats
full outer join (
	-- 分拣
	select
		'2024-01-08' dt,
		recent_days,
		city_id,
		city_name,
		sort_count
	from (
		select
			1 recent_days,
			city_id,
			case when city_id = 110100 then '北京市'
				when city_id = 120100 then '天津市'
				when city_id = 310100 then '上海市'
				when city_id = 500100 then '重庆市'
				else city_name end city_name,
			sum(sort_count) sort_count
		from dws_trans_org_sort_1d
		where dt = '2024-01-08'
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
			sum(sort_count) sort_count
		from dws_trans_org_sort_nd
		where dt = '2024-01-08'
		group by
			recent_days,
			city_id,
			city_name
	) tmp3		
) sort_stats
on receive_deliver_stats.dt = sort_stats.dt
	and receive_deliver_stats.recent_days = sort_stats.recent_days
	and receive_deliver_stats.city_id = sort_stats.city_id
	and receive_deliver_stats.city_name = sort_stats.city_name;
