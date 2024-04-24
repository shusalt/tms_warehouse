#!/bin/bash

APP=tms
HIVE_PATH=/usr/hive-2.1/bin


if [ $# -lt 1 ]; then
	echo "必须传入all/表名..."
	exit
fi



# 判断日期是否传入
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)




# 交易域机构货物类型粒度下单1日汇总表
dws_trade_org_cargo_type_order_1d_sql="
insert overwrite table dws_trade_org_cargo_type_order_1d partition (dt='$datestr')
select
	cargo_type,
	cargo_type_name,
	city_id,
	city_name,
	org_id,
	org_name,
	count(order_id) order_count,
	sum(amount) order_amount
from (
	select
		cargo_type,
		cargo_type_name,
		city_id,
		city_name,
		org_id,
		org_name,
		order_id,
		amount
	from (
		select
			order_id,
			cargo_type,
			cargo_type_name,
			sender_city_id city_id,
			sender_district_id,
			max(amount) amount
		from (
			select
				order_id,
				cargo_type,
				cargo_type_name,
				sender_city_id,
				sender_district_id,
				amount
			from dwd_trade_order_detail_inc
			where dt = '$datestr'
		) tmp1
		group by 
			order_id,
			cargo_type,
			cargo_type_name,
			sender_city_id,
			sender_district_id
	) order_detail
	left join (
		select
			id,
			name city_name
		from dim_region_full
		where dt = '2024-01-07'
	) city_info
	on order_detail.city_id =  city_info.id
	left join (
		select
			region_id,
			id org_id,
			org_name
		from dim_organ_full
		where dt = '2024-01-07'
	) org_info
	on order_detail.sender_district_id = org_info.region_id	
) tb1
group by
	cargo_type,
	cargo_type_name,
	city_id,
	city_name,
	org_id,
	org_name;
"






# 物流域转运站粒度揽收1日汇总表
dws_trans_org_receive_1d_sql="
insert overwrite table dws_trans_org_receive_1d partition (dt = '$datestr')
select
	province_id,
	province_name,
	city_id,
	city_name,
	org_id,
	org_name,
	count(order_id) order_count,
	sum(amount) order_amount
from (
	select
		order_id,
		sender_province_id province_id,
		province_name,
		sender_city_id city_id,
		city_name,
		org_id,
		org_name,
		amount
	from (
		select
			order_id,
			sender_province_id,
			sender_city_id,
			sender_district_id,
			max(amount) amount
		from dwd_trans_receive_detail_inc
		where dt = '$datestr'
		group by
			order_id,
			sender_province_id,
			sender_city_id,
			sender_district_id
	) receive_info
	left join (
		select
			id,
			name province_name
		from dim_region_full
		where dt = '2024-01-07'
	) dim_province
	on receive_info.sender_province_id = dim_province.id
	left join (
		select
			id,
			name city_name
		from dim_region_full
		where dt = '2024-01-07'
	) dim_city
	on receive_info.sender_city_id = dim_city.id
	left join (
		select
			region_id,
			id org_id,
			org_name
		from dim_organ_full
	) dim_org
	on receive_info.sender_district_id = dim_org.region_id
) tmp
group by
	province_id,
	province_name,
	city_id,
	city_name,
	org_id,
	org_name;
"





# 物流域发单1日汇总表
dws_trans_dispatch_1d_sql="
insert overwrite table dws_trans_dispatch_1d partition (dt = '$datestr')
select
	count(order_id) order_count,
	sum(amount) order_amount
from (
	select
		order_id,
		max(amount) amount
	from dwd_trans_dispatch_detail_inc
	where dt = '$datestr'
	group by
		order_id
) dispatch_info;
"






# 物流域机构卡车类别粒度运输最近1日汇总表
dws_trans_org_truck_model_type_trans_finish_1d_sql="
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d partition (dt='$datestr')
select
	org_id,
	ord_name,
	truck_model_type,
	truck_model_type_name,
	count(trans_finish_info.id) truck_finish_count,
	sum(actual_distance) trans_finish_distance,
	sum(finish_dur_sec) finish_dur_sec
from (
	select
		id,
		start_org_id org_id,
		start_org_name ord_name,
		truck_id,
		actual_distance,
		finish_dur_sec
	from dwd_trans_trans_finish_inc
	where dt = '$datestr'	
) trans_finish_info
left join (
	select
		id,
		truck_model_type,
		truck_model_type_name
	from dim_truck_full
	where dt = '2024-01-07'
) dim_truck
on trans_finish_info.truck_id = dim_truck.id
group by
	org_id,
	ord_name,
	truck_model_type,
	truck_model_type_name;
"





# 物流域机构粒度分拣1日汇总表
dws_trans_org_sort_1d_sql="
insert overwrite table dws_trans_org_sort_1d partition (dt='$datestr')
select
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	count(id) sort_count
from (
	select
		sort_info.org_id,
		org_name,
		if(org_level=1, city_for_level1.id, province_for_level1.id) city_id,
		if(org_level=1, city_for_level1.name, province_for_level1.name) city_name,
		if(org_level=1, province_for_level1.id, province_for_level2.id) province_id,
		if(org_level=1, province_for_level1.name, province_for_level2.name) province_name,
		sort_info.id
	from (
		select
			id,
			org_id
		from dwd_bound_sort_inc
		where dt = '$datestr'
	) sort_info
	left join (
		select
			id org_id,
			org_name,
			org_level,
			region_id
		from dim_organ_full
		where dt = '2024-01-07'
	) organ_info
	on sort_info.org_id = organ_info.org_id
	left join (
		select
			id,
			name,
			parent_id
		from dim_region_full
		where dt = '2024-01-07'
	) city_for_level1
	on organ_info.region_id = city_for_level1.id
	left join (
		select
			id,
			name,
			parent_id
		from dim_region_full
		where dt = '2024-01-07'
	) province_for_level1
	on city_for_level1.parent_id = province_for_level1.id
	left join (
		select
			id,
			name,
			parent_id
		from dim_region_full
		where dt = '2024-01-07'
	) province_for_level2
	on province_for_level1.parent_id = province_for_level2.id
) tms
group by
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name;
"







# 物流域转运站粒度派送成功1日汇总表
dws_trans_org_deliver_suc_1d_sql="
insert overwrite table dws_trans_org_deliver_suc_1d partition (dt='$datestr')
select
	org_id,
	org_name,
	province_id,
	province_name,
	city_id,
	city_name,
	count(order_id) order_count
from (
	select
		org_id,
		org_name,
		province_id,
		province_name,
		city_id,
		city_name,
		order_id
	from (
		select
			receiver_province_id province_id,
			receiver_city_id city_id,
			receiver_district_id district_id,
			max(order_id) order_id
		from dwd_trans_deliver_suc_detail_inc
		where dt = '$datestr'
		group by
			receiver_province_id,
			receiver_city_id,
			receiver_district_id
	) deliver_info
	left join (
		select
			id,
			name province_name
		from dim_region_full
		where dt = '2024-01-07'
	) dim_province
	on deliver_info.province_id = dim_province.id
	left join (
		select
			id,
			name city_name
		from dim_region_full
		where dt = '2024-01-07'
	) dim_city
	on deliver_info.city_id = dim_city.id
	left join (
		select
			region_id,
			id org_id,
			org_name
		from dim_organ_full
		where dt = '2024-01-07'
	) dim_organ
	on deliver_info.district_id = dim_organ.region_id	
) tmp
group by
	org_id,
	org_name,
	province_id,
	province_name,
	city_id,
	city_name;
"


exec_func(){
	echo "use tms;${!1}"
	$HIVE_PATH/hive -e "use tms;${!1}"
}



case $1 in
	"all")
		$HIVE_PATH/hive -e "use tms;${dws_trade_org_cargo_type_order_1d_sql}${dws_trans_org_receive_1d_sql}${dws_trans_dispatch_1d_sql}${dws_trans_org_truck_model_type_trans_finish_1d_sql}${dws_trans_org_sort_1d_sql}${dws_trans_org_deliver_suc_1d_sql}"
		;;
	dws_trade_org_cargo_type_order_1d | dws_trans_org_receive_1d | dws_trans_dispatch_1d | dws_trans_org_truck_model_type_trans_finish_1d | dws_trans_org_sort_1d | dws_trans_org_deliver_suc_1d)
		sql="${1}_sql"
		exec_func $sql
		;;
esac