#!/bin/bash

APP=tms
HIVE_PATH=/usr/hive-2.1/bin

if [ $# -le 1 ]; then
	echo "必须传入all/表名..."
fi

[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)


# 交易域机构货物类型粒度下单n日汇总表
dws_trade_org_cargo_type_order_nd_sql="
insert overwrite table dws_trade_org_cargo_type_order_nd partition (dt='$datestr')
select
	org_id,
	org_name,
	city_id,
	city_name,
	cargo_type,
	cargo_type_name,
	recent_days,
	sum(order_count) order_count,
	sum(order_amount) order_amount
from (
	select
		org_id,
		org_name,
		city_id,
		city_name,
		cargo_type,
		cargo_type_name,
		order_count,
		order_amount,
		recent_days
	from dws_trade_org_cargo_type_order_1d
	lateral view explode(array(7, 30)) tmp as recent_days
	where dt >= date_add('$datestr', -recent_days+1)
) info
group by 
	org_id,
	org_name,
	city_id,
	city_name,
	cargo_type,
	cargo_type_name,
	recent_days;
"




# 物流域转运站粒度揽收n日汇总表
dws_trans_org_receive_nd_sql="
insert overwrite table dws_trans_org_receive_nd partition (dt='$datestr')
select
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	recent_days,
	sum(order_count) order_count,
	sum(order_amount) order_amount
from (
	select
		org_id,
		org_name,
		city_id,
		city_name,
		province_id,
		province_name,
		order_count,
		order_amount,
		recent_days
	from dws_trans_org_receive_1d
	lateral view explode(array(7, 30)) tmp as recent_days
	where dt >= date_add('$date_add', -recent_days+1)
) info
group by
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	recent_days;
"





# 物流域发单n日汇总表
dws_trans_dispatch_nd_sql="
insert overwrite table dws_trans_dispatch_nd partition (dt='$datestr')
select
	recent_days,
	sum(order_count),
	sum(order_amount)
from (
	select
		order_count,
		order_amount,
		recent_days
	from dws_trans_dispatch_1d
	lateral view explode(array(7, 30)) tmp as recent_days
	where dt >= date_add('$datestr', -recent_days+1)
) info
group by
	recent_days;
"



# 物流域班次粒度转运完成最近n日汇总表
dws_trans_shift_trans_finish_nd_sql="
insert overwrite table dws_trans_shift_trans_finish_nd partition (dt='$datestr')
select
	shift_id,
	city_id,
	city_name,
	line_id,
	line_name,
	start_org_id org_id,
	start_org_name org_name,
	driver1_emp_id,
	driver1_name,
	driver2_emp_id,
	driver2_name,
	truck_model_type,
	truck_model_type_name,
	recent_days,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	trans_finish_order_count,
	trans_finish_delay_count
from (
	-- 先聚合在关联维度名称，事实表里已有维度id
	select
		shift_id,
		line_id,
		start_org_id,
		start_org_name,
		driver1_emp_id,
		driver1_name,
		driver2_emp_id,
		driver2_name,
		truck_id,
		recent_days,
		count(id) trans_finish_count,
		sum(actual_distance) trans_finish_distance,
		sum(finish_dur_sec) trans_finish_dur_sec,
		sum(order_num) trans_finish_order_count,
		sum(if(actual_end_time > estimate_end_time, 1, 0)) trans_finish_delay_count
	from dwd_trans_trans_finish_inc
	lateral view explode(array(7, 30)) tmp as recent_days
	where dt >= date_add('$datestr', -recent_days+1)
	group by
		shift_id,
		line_id,
		start_org_id,
		start_org_name,
		driver1_emp_id,
		driver1_name,
		driver2_emp_id,
		driver2_name,
		truck_id,
		recent_days
) trans_info
left join (
	select
		organ_info.id,
		if(org_level=1, dim_city.id, dim_province.id) city_id,
		if(org_level=1, dim_city.name, dim_province.name) city_name
	from (
		select
			id,
			org_level,
			region_id
		from dim_organ_full
		where dt = '2024-01-07'
	) organ_info
	left join (
		select
			id,
			name,
			parent_id
		from dim_region_full
		where dt = '2024-01-07'
	) dim_city
	on organ_info.region_id = dim_city.id
	left join (
		select
			id,
			name
		from dim_region_full
		where dt = '2024-01-07'
	) dim_province
	on dim_city.parent_id = dim_province.id
) city_info
on trans_info.start_org_id = city_info.id
left join (
	select
		id,
		line_name
	from dim_shift_full
	where dt = '2024-01-07'
) shift_info
on trans_info.shift_id = shift_info.id
left join (
	select
		id,
		truck_model_type,
		truck_model_type_name
	from dim_truck_full
	where dt = '2024-01-07'
) truck_info
on trans_info.truck_id = truck_info.id;
"




# 物流域转运站粒度派送成功n日汇总表
dws_trans_org_deliver_suc_nd_sql="
insert overwrite table dws_trans_org_deliver_suc_nd partition (dt='$datestr')
select
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	recent_days,
	sum(order_count) order_count
from (
	select
		org_id,
		org_name,
		city_id,
		city_name,
		province_id,
		province_name,
		recent_days,
		order_count
	from dws_trans_org_deliver_suc_1d
	lateral view explode(array(7, 30)) tmp as recent_days
	where dt >= date_add('$datestr', -recent_days+1)
) info
group by
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	recent_days;
"






# 物流域机构粒度分拣n日汇总表
dws_trans_org_sort_nd_sql="
insert overwrite table dws_trans_org_sort_nd partition (dt='$datestr')
select
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	recent_days,
	sum(sort_count) sort_count
from (
	select
		org_id,
		org_name,
		city_id,
		city_name,
		province_id,
		province_name,
		sort_count,
		recent_days
	from dws_trans_org_sort_1d
	lateral view explode(array(7, 30)) tmp as recent_days
	where dt >= date_add('$datestr', -recent_days+1)	
) info
group by
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	recent_days;
"



exec_func(){
	echo "use tms;${!1}"
	$HIVE_PATH/hive -e "use tms;${!1}"
}



case $1 in
	"all")
		$HIVE_PATH/hive -e "use tms;${dws_trade_org_cargo_type_order_nd_sql}${dws_trans_org_receive_nd_sql}${dws_trans_dispatch_nd_sql}${dws_trans_shift_trans_finish_nd_sql}${dws_trans_org_deliver_suc_nd_sql}${dws_trans_org_sort_nd_sql}"
		;;
	dws_trade_org_cargo_type_order_nd | dws_trans_org_receive_nd | dws_trans_dispatch_nd | dws_trans_shift_trans_finish_nd | dws_trans_org_deliver_suc_nd | dws_trans_org_sort_nd)
		sql="${1}_sql"
		exec_func $sql
		;;
esac 