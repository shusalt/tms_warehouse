#!/bin/bash

APP=tms
HIVE_PATH=/usr/hive-2.1/bin

if [ $# -lt 1 ]; then
	echo "必须输入一个表名或者all"
	exit
fi


[ $2 ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)



# 运单相关统计
ads_trans_order_stats_sql="
insert overwrite table ads_trans_order_stats
select
	dt,
	recent_days,
	receive_order_count,
	receive_order_amount,
	dispatch_order_count,
	dispatch_order_amount
from ads_trans_order_stats
union
select
	'$datestr' dt,
	receive_stats.recent_days,
	receive_order_count,
	receive_order_amount,
	dispatch_order_count,
	dispatch_order_amount
from (
	select
		1 recent_days,
		sum(order_count) receive_order_count,
		sum(order_amount) receive_order_amount
	from dws_trans_org_receive_1d
	where dt = '$datestr'
	union
	select
		recent_days,
		sum(order_count) receive_order_count,
		sum(order_amount) receive_order_amount
	from dws_trans_org_receive_nd
	where dt = '$datestr'
	group by recent_days
) receive_stats
join (
	select
		1 recent_days,
		sum(order_count) dispatch_order_count,
		sum(order_amount) dispatch_order_amount
	from dws_trans_dispatch_1d
	where dt = '$datestr'
	union
	select
		recent_days,
		sum(order_count) dispatch_order_count,
		sum(order_amount) dispatch_order_amount
	from dws_trans_dispatch_nd
	where dt = '$datestr'
	group by recent_days
) dispatch_stats
on receive_stats.recent_days = dispatch_stats.recent_days;
"




# 物流主题运输相关统计
ads_trans_stats_sql="
insert overwrite table ads_trans_stats
select
	dt,
	recent_days,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec	
from ads_trans_stats
union
select
	'$datestr' dt,
	recent_days,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec
from (
	select
		1 recent_days,
		sum(trans_finish_count) trans_finish_count,
		sum(trans_finish_distance) trans_finish_distance,
		sum(trans_finish_dur_sec) trans_finish_dur_sec
	from dws_trans_org_truck_model_type_trans_finish_1d
	where dt = '$datestr'
	union
	select
		recent_days,
		sum(trans_finish_count) trans_finish_count,
		sum(trans_finish_distance) trans_finish_distance,
		sum(trans_finish_dur_sec) trans_finish_dur_sec
	from dws_trans_shift_trans_finish_nd
	where dt = '$datestr'
	group by recent_days
) stats;
"





# 物流主题历史至今运单统计
ads_trans_order_td_stats_sql="
insert overwrite table ads_trans_order_td_stats
select
	dt,
	bounding_order_count,
	bounding_order_amount
from ads_trans_order_td_stats
union
select
	dispatch_stats.dt,
	(dispatch_order_count - bound_finish_order_count) bounding_order_count,
	(dispatch_order_amount - bound_finish_order_amount)  bounding_order_amount
from (
	select
		'$datestr' dt,
		order_count dispatch_order_count,
		order_amount dispatch_order_amount
	from dws_trans_dispatch_td
	where dt = '$datestr'	
) dispatch_stats
join (
	select
		'$datestr' dt,
		order_count bound_finish_order_count,
		order_amount bound_finish_order_amount
	from dws_trans_bound_finish_td
	where dt = '$datestr'
) bound_finish_stats
on dispatch_stats.dt = bound_finish_stats.dt;
"





# 运单主题运单综合统计
ads_order_stats_sql="
insert overwrite table ads_order_stats
select
	dt,
	recent_days,
	order_count,
	order_amount
from ads_order_stats
union
select
	'$datestr' dt,
	recent_days,
	order_count,
	order_amount
from (
	select
		1 recent_days,
		sum(order_count) order_count,
		sum(order_amount) order_amount
	from dws_trade_org_cargo_type_order_1d
	where dt = '$datestr'
	union
	select
		recent_days,
		sum(order_count) order_count,
		sum(order_amount) order_amount	
	from dws_trade_org_cargo_type_order_nd
	where dt = '$datestr'
	group by recent_days	
) stats;
"




# 各类型货物运单统计
ads_order_cargo_type_stats_sql="
insert overwrite table ads_order_cargo_type_stats
select
	dt,
	recent_days,
	cargo_type,
	cargo_type_name,
	order_count,
	order_amount
from ads_order_cargo_type_stats
union
select
	'$datestr' dt,
	recent_days,
	cargo_type,
	cargo_type_name,
	order_count,
	order_amount	
from (
	select
		1 recent_days,
		cargo_type,
		cargo_type_name,
		sum(order_count) order_count,
		sum(order_amount) order_amount
	from dws_trade_org_cargo_type_order_1d
	where dt = '$datestr'
	group by
		cargo_type,
		cargo_type_name
	union
	select
		recent_days,
		cargo_type,
		cargo_type_name,
		sum(order_count) order_count,
		sum(order_amount) order_amount
	from dws_trade_org_cargo_type_order_nd
	where dt = '$datestr'
	group by
		recent_days,
		cargo_type,
		cargo_type_name	
) stats;
"





# 城市分析
ads_city_stats_sql="
with city_order_stats as (
	-- 城市下单统计指标
	select
		'$datestr' dt,
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
		where dt = '$datestr'
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
		where dt = '$datestr'
		group by recent_days, city_id, city_name
	) tmp_tb1		
),
city_trans_stats as (
	-- 城市运输指标统计
	select
		'$datestr' dt,
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
				where dt = '$datestr'
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
		where dt = '$datestr'
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
"





# 机构分析
ads_org_stats_sql="
with org_order_stats as (
	-- 机构下单统计指标
	select
		'$datestr' dt,
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
		where dt = '$datestr'
		group by org_id, org_name
		union
		select
			recent_days,
			org_id,
			org_name,
			sum(order_count) order_count,
			sum(order_amount) order_amount
		from dws_trade_org_cargo_type_order_nd
		where dt = '$datestr'
		group by recent_days, org_id, org_name
	) tmp_tb1		
),
org_trans_stats as (
	-- 机构运输指标统计
	select
		'$datestr' dt,
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
		where dt = '$datestr'
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
		where dt = '$datestr'
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
"





# 班次分析
ads_shift_stats_sql="
insert overwrite table ads_shift_stats
select
	dt,
	recent_days,
	shift_id,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	trans_finish_order_count	
from ads_shift_stats
union
select
	'$datestr' dt,
	recent_days,
	shift_id,
	sum(trans_finish_count) trans_finish_count,
	sum(trans_finish_distance) trans_finish_distance,
	sum(trans_finish_dur_sec) trans_finish_dur_sec,
	sum(trans_finish_order_count) trans_finish_order_count
from dws_trans_shift_trans_finish_nd
where dt = '$datestr'
group by
	recent_days,
	shift_id;
"




# 线路分析
ads_line_stats_sql="
insert overwrite table ads_line_stats
select
	dt,
	recent_days,
	line_id,
	line_name,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	trans_finish_order_count
from ads_line_stats
union
select
	'$datestr' dt,
	recent_days,
	line_id,
	line_name,
	sum(trans_finish_count) trans_finish_count,
	sum(trans_finish_distance) trans_finish_distance,
	sum(trans_finish_dur_sec) trans_finish_dur_sec,
	sum(trans_finish_order_count) trans_finish_order_count
from dws_trans_shift_trans_finish_nd
where dt = '$datestr'
group by
	recent_days,
	line_id,
	line_name;
"






# 司机分析
ads_driver_stats_sql="
insert overwrite table ads_driver_stats
select
	dt,
	recent_days,
	driver_emp_id,
	driver_name,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	avg_trans_finish_distance,
	avg_trans_finish_dur_sec,
	trans_finish_late_count
from ads_driver_stats
union
select
	'$datestr' dt,
	recent_days,
	driver1_emp_id	driver_id,
	driver1_name driver_name,
	sum(trans_finish_count) trans_finish_count,
	sum(trans_finish_distance) trans_finish_distance,
	sum(trans_finish_dur_sec) trans_finish_dur_sec,
	sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
	sum(trans_finish_dur_sec) / sum(trans_finish_count) avg_trans_finish_dur_sec,
	sum(trans_finish_delay_count) trans_finish_late_count
from dws_trans_shift_trans_finish_nd
where dt = '$datestr'
group by
	recent_days,
	driver1_emp_id,
	driver1_name;
"





# 快递综合统计
ads_express_stats_sql="
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
		'$datestr' dt,
		recent_days,
		order_count
	from (
		select
			1 recent_days,
			sum(order_count) order_count
		from dws_trans_org_deliver_suc_1d
		where dt = '$datestr'
		union
		select
			recent_days,
			sum(order_count) order_count
		from dws_trans_org_deliver_suc_nd
		where dt = '$datestr'
		group by
			recent_days	
	) tmp1
) deliver_stats
full outer join (
	select
		'$datestr' dt,
		recent_days,
		sort_count
	from (
		select
			1 recent_days,
			sum(sort_count) sort_count
		from dws_trans_org_sort_1d
		where dt = '$datestr'
		union
		select
			recent_days,
			sum(sort_count) sort_count
		from dws_trans_org_sort_nd
		where dt = '$datestr'
		group by recent_days	
	) tmp2
) sort_stats
on deliver_stats.dt = sort_stats.dt and deliver_stats.recent_days = sort_stats.recent_days;
"




# 各省份快递统计
ads_express_province_stats_sql="
insert overwrite table ads_express_province_stats
select
	dt,
	recent_days,
	province_id,
	province_name,
	receive_order_count,
	receive_order_amount,
	deliver_suc_count,
	sort_count
from ads_express_province_stats
union
select
	nvl(receive_deliver_stats.dt, sort_stats.dt) dt,
	nvl(receive_deliver_stats.recent_days, sort_stats.recent_days) recent_days,
	nvl(receive_deliver_stats.province_id, sort_stats.province_id) province_id,
	nvl(receive_deliver_stats.province_name, sort_stats.province_name) province_name,
	receive_order_count,
	receive_order_amount,
	deliver_suc_count,
	sort_count
from (
	select
		nvl(receive_stats.dt, deliver_stats.dt) dt,
		nvl(receive_stats.recent_days, deliver_stats.recent_days) recent_days,
		nvl(receive_stats.province_id, deliver_stats.province_id) province_id,
		nvl(receive_stats.province_name, deliver_stats.province_name) province_name,
		receive_order_count,
		receive_order_amount,
		deliver_suc_count	
	from (
		-- 揽收
		select
			'$datestr' dt,
			recent_days,
			province_id,
			province_name,
			receive_order_count,
			receive_order_amount
		from (
			select
				1 recent_days,
				province_id,
				province_name,
				sum(order_count) receive_order_count,
				sum(order_amount) receive_order_amount
			from dws_trans_org_receive_1d
			where dt = '$datestr'
			group by
				province_id,
				province_name
			union
			select
				recent_days,
				province_id,
				province_name,
				sum(order_count) receive_order_count,
				sum(order_amount) receive_order_amount
			from dws_trans_org_receive_nd
			where dt = '$datestr'
			group by
				recent_days,
				province_id,
				province_name	
		) tmp1	
	) receive_stats
	full outer join (
		-- 派送成功
		select
			'$datestr' dt,
			recent_days,
			province_id,
			province_name,
			deliver_suc_count
		from (
			select
				1 recent_days,
				province_id,
				province_name,
				sum(order_count) deliver_suc_count
			from dws_trans_org_deliver_suc_1d
			where dt = '$datestr'
			group by
				province_id,
				province_name
			union
			select
				recent_days,
				province_id,
				province_name,
				sum(order_count) deliver_suc_count
			from dws_trans_org_deliver_suc_nd
			where dt = '$datestr'
			group by
				recent_days,
				province_id,
				province_name	
		) tmp2
	) deliver_stats
	on receive_stats.dt = deliver_stats.dt
		and receive_stats.recent_days = deliver_stats.recent_days
		and receive_stats.province_id = deliver_stats.province_id
		and receive_stats.province_name = deliver_stats.province_name
) receive_deliver_stats
full outer join (
	-- 分拣
	select
		'$datestr' dt,
		recent_days,
		province_id,
		province_name,
		sort_count
	from (
		select
			1 recent_days,
			province_id,
			province_name,
			sum(sort_count) sort_count
		from dws_trans_org_sort_1d
		where dt = '$datestr'
		group by
			province_id,
			province_name
		union
		select
			recent_days,
			province_id,
			province_name,
			sum(sort_count) sort_count
		from dws_trans_org_sort_nd
		where dt = '$datestr'
		group by
			recent_days,
			province_id,
			province_name
	) tmp3	
) sort_stats
on receive_deliver_stats.dt = sort_stats.dt
	and receive_deliver_stats.recent_days = sort_stats.recent_days
	and receive_deliver_stats.province_id = sort_stats.province_id
	and receive_deliver_stats.province_name = sort_stats.province_name;
"




# 各城市快递统计
ads_express_city_stats_sql="
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
			'$datestr' dt,
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
			where dt = '$datestr'
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
			where dt = '$datestr'
			group by
				recent_days,
				city_id,
				city_name	
		) tmp1
	) receive_stats
	full outer join (
		-- 派送成功
		select
			'$datestr' dt,
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
			where dt = '$datestr'
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
			where dt = '$datestr'
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
		'$datestr' dt,
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
		where dt = '$datestr'
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
		where dt = '$datestr'
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
"





# 各机构快递统计
ads_express_org_stats_sql="
insert overwrite table ads_express_org_stats
select
	dt,
	recent_days,
	org_id,
	org_name,
	receive_order_count,
	receive_order_amount,
	deliver_suc_count,
	sort_count
from ads_express_org_stats
union
select
	nvl(receive_deliver_stats.dt, sort_stats.dt) dt,
	nvl(receive_deliver_stats.recent_days, sort_stats.recent_days) recent_days,
	nvl(receive_deliver_stats.org_id, sort_stats.org_id) org_id,
	nvl(receive_deliver_stats.org_name, sort_stats.org_name) org_name,
	receive_order_count,
	receive_order_amount,
	deliver_suc_count,
	sort_count
from (
	select
		nvl(receive_stats.dt, deliver_stats.dt) dt,
		nvl(receive_stats.recent_days, deliver_stats.recent_days) recent_days,
		nvl(receive_stats.org_id, deliver_stats.org_id) org_id,
		nvl(receive_stats.org_name, deliver_stats.org_name) org_name,
		receive_order_count,
		receive_order_amount,
		deliver_suc_count	
	from (
		-- 揽收
		select
			'2024-01-08' dt,
			recent_days,
			org_id,
			org_name,
			receive_order_count,
			receive_order_amount
		from (
			select
				1 recent_days,
				org_id,
				org_name,
				sum(order_count) receive_order_count,
				sum(order_amount) receive_order_amount
			from dws_trans_org_receive_1d
			where dt = '2024-01-08'
			group by
				org_id,
				org_name
			union
			select
				recent_days,
				org_id,
				org_name,
				sum(order_count) receive_order_count,
				sum(order_amount) receive_order_amount
			from dws_trans_org_receive_nd
			where dt = '2024-01-08'
			group by
				recent_days,
				org_id,
				org_name	
		) tmp1	
	) receive_stats
	full outer join (
		-- 派送成功
		select
			'2024-01-08' dt,
			recent_days,
			org_id,
			org_name,
			deliver_suc_count
		from (
			select
				1 recent_days,
				org_id,
				org_name,
				sum(order_count) deliver_suc_count
			from dws_trans_org_deliver_suc_1d
			where dt = '2024-01-08'
			group by
				org_id,
				org_name
			union
			select
				recent_days,
				org_id,
				org_name,
				sum(order_count) deliver_suc_count
			from dws_trans_org_deliver_suc_nd
			where dt = '2024-01-08'
			group by
				recent_days,
				org_id,
				org_name	
		) tmp2
	) deliver_stats
	on receive_stats.dt = deliver_stats.dt
		and receive_stats.recent_days = deliver_stats.recent_days
		and receive_stats.org_id = deliver_stats.org_id
		and receive_stats.org_name = deliver_stats.org_name
) receive_deliver_stats
full outer join (
	-- 分拣
	select
		'2024-01-08' dt,
		recent_days,
		org_id,
		org_name,
		sort_count
	from (
		select
			1 recent_days,
			org_id,
			org_name,
			sum(sort_count) sort_count
		from dws_trans_org_sort_1d
		where dt = '2024-01-08'
		group by
			org_id,
			org_name
		union
		select
			recent_days,
			org_id,
			org_name,
			sum(sort_count) sort_count
		from dws_trans_org_sort_nd
		where dt = '2024-01-08'
		group by
			recent_days,
			org_id,
			org_name
	) tmp3	
) sort_stats
on receive_deliver_stats.dt = sort_stats.dt
	and receive_deliver_stats.recent_days = sort_stats.recent_days
	and receive_deliver_stats.org_id = sort_stats.org_id
	and receive_deliver_stats.org_name = sort_stats.org_name;
"



case $1 in
	"all")
		$HIVE_PATH/hive -e "use tms;${ads_trans_order_stats_sql}${ads_trans_stats_sql}${ads_trans_order_td_stats_sql}${ads_order_stats_sql}${ads_order_cargo_type_stats_sql}${ads_city_stats_sql}${ads_org_stats_sql}${ads_shift_stats_sql}${ads_line_stats_sql}${ads_driver_stats_sql}${ads_express_stats_sql}${ads_express_province_stats_sql}${ads_express_city_stats_sql}${ads_express_org_stats_sql}"
		;;
	ads_trans_order_stats | ads_trans_stats | ads_trans_order_td_stats | ads_order_stats | ads_order_cargo_type_stats | ads_city_stats | ads_org_stats | ads_shift_stats | ads_line_stats | ads_driver_stats | ads_express_stats | ads_express_province_stats | ads_express_city_stats | ads_express_org_stats)
		sql="${1}_sql"
		$HIVE_PATH/hive -e "use tms;${!sql}"
		;;
esac