#!/bin/bash

APP=tms
HIVE_PATH=/usr/hive-2.1/bin


if [ $# -lt 2 ]; then
	echo "必须传入all/表名以及上线日期"
	exit
fi


# 交易域订单明细事务事实表(dwd_trade_order_detail_inc)
dwd_trade_order_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_cargo.order_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(order_cargo.order_time, 'yyyy-MM-dd') dt
from (
	-- 订单明细表
	select
		data.id,
		data.order_id,
		data.cargo_type,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		data.create_time order_time,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	-- 订单表
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance
	from ods_order_info_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id;
"



# 交易域支付成功事务事实表(dwd_trade_pay_suc_detail_inc)
dwd_trade_pay_suc_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trade_pay_suc_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_info.payment_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.payment_type,
	payment_type_dic.payment_type_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(payment_time, 'yyyy-MM-dd') dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.payment_type,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time payment_time
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert' 
		and data.status not in ('60010', '60999')
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id
left join (
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;
"


# 交易域取消运单事务事实表(dwd_trade_order_cancel_detail_inc)
dwd_trade_order_cancel_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trade_order_cancel_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_info.cancel_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(cancel_time, 'yyyy-MM-dd') dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time cancel_time
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert' 
		and data.status = '60999'
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id;
"


# 交易域运单累积快照事实表(dwd_trade_order_process_inc)
dwd_trade_order_process_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trade_order_process_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_cargo.order_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.payment_type,
	payment_type_dic.payment_type_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	order_cargo.ts,
	date_format(order_cargo.order_time, 'yyyy-MM-dd') start_date,
	order_info.end_date,
	order_info.end_date dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		data.create_time order_time,
		ts
	from ods_order_cargo_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.payment_type,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		if(data.status = '60080' or data.status = '60999', 
			date_format(data.update_time, 'yyyy-MM-dd'), 
			'9999-12-31') end_date
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0'
		and type = 'bootstrap-insert'
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id
left join (
	-- 支付类型
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;
"



# 物流域揽收事务事实表(dwd_trans_receive_detail_inc)
dwd_trans_receive_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trans_receive_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_info.receive_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.payment_type,
	payment_type_dic.payment_type_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(receive_time, 'yyyy-MM-dd') dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.payment_type,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time receive_time
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert' 
		and data.status not in ('60010', '60020', '60999')
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id
left join (
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;
"



# 物流域发单事务事实表(dwd_trans_dispatch_detail_inc)
dwd_trans_dispatch_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trans_dispatch_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_info.dispatch_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.payment_type,
	payment_type_dic.payment_type_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(dispatch_time, 'yyyy-MM-dd') dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.payment_type,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time dispatch_time
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert' 
		and data.status not in ('60010', '60020', '60030', '60040', '60999')
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id
left join (
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;
"





# 物流域转运完成事务事实表(dwd_trans_bound_finish_detail_inc)
dwd_trans_bound_finish_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trans_bound_finish_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_info.bound_finish_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.payment_type,
	payment_type_dic.payment_type_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(bound_finish_time, 'yyyy-MM-dd') dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.payment_type,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time bound_finish_time
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert' 
		and data.status not in ('60010', '60020', '60030', '60040', '60050', '60999')
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id
left join (
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;
"







# 物流域派生完成事务事实表(dwd_trans_deliver_suc_detail_inc)
dwd_trans_deliver_suc_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trans_deliver_suc_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_info.deliver_suc_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.payment_type,
	payment_type_dic.payment_type_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(deliver_suc_time, 'yyyy-MM-dd') dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.payment_type,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time deliver_suc_time
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert' 
		and data.status not in ('60010', '60020', '60030', '60040', '60050', '60060', '60999')
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id
left join (
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;
"



# 物流域签收事务事实表(dwd_trans_sign_detail_inc)
dwd_trans_sign_detail_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trans_sign_detail_inc partition (dt)
select
	order_cargo.id,
	order_cargo.order_id,
	order_cargo.cargo_type,
	cargo_type_dic.cargo_type_name,
	order_cargo.volumn_length,
	order_cargo.volumn_width,
	order_cargo.volumn_height,
	order_cargo.weight,
	order_info.sign_time,
	order_info.order_no,
	order_info.status,
	status_dic.status_name,
	order_info.collect_type,
	collect_type_dic.collect_type_name,
	order_info.user_id,
	order_info.receiver_complex_id,
	order_info.receiver_province_id,
	order_info.receiver_city_id,
	order_info.receiver_district_id,
	order_info.receiver_name,
	order_info.sender_complex_id,
	order_info.sender_province_id,
	order_info.sender_city_id,
	order_info.sender_district_id,
	order_info.sender_name,
	order_info.payment_type,
	payment_type_dic.payment_type_name,
	order_info.cargo_num,
	order_info.amount,
	order_info.estimate_arrive_time,
	order_info.distance,
	ts,
	date_format(sign_time, 'yyyy-MM-dd') dt
from (
	select
		data.id,
		data.order_id,
		data.cargo_type,
		-- data.cargo_type_name,
		data.volume_length volumn_length,
		data.volume_width volumn_width,
		data.volume_height volumn_height,
		data.weight,
		ts
	from ods_order_cargo_inc
	where dt = '$2' and data.is_deleted = '0' and type = 'bootstrap-insert'
) order_cargo
inner join (
	select
		data.id order_id,
		data.order_no,
		data.status,
		-- data.status_name,
		data.collect_type,
		-- data.collect_type_name,
		data.user_id,
		data.receiver_complex_id,
		data.receiver_province_id,
		data.receiver_city_id,
		data.receiver_district_id,
		concat(substring(data.receiver_name,1,1), '*') receiver_name,
		data.sender_complex_id,
		data.sender_province_id,
		data.sender_city_id,
		data.sender_district_id,
		concat(substring(data.sender_name,1, 1), '*') sender_name,
		data.payment_type,
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time sign_time
	from ods_order_info_inc
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'bootstrap-insert' 
		and data.status not in ('60010', '60020', '60030', '60040', '60050', '60060', '60070', '60999')
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id
left join (
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;
"




# 物流域运输完成事务事实表(dwd_trans_trans_finish_inc)
dwd_trans_trans_finish_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trans_trans_finish_inc partition(dt)
select
	info.id,
	info.shift_id,
	info.line_id,
	info.start_org_id,
	info.start_org_name,
	info.end_org_id,
	info.end_org_name,
	info.order_num,
	info.driver1_emp_id,
	info.driver1_name,
	info.driver2_emp_id,
	info.driver2_name,
	info.truck_id,
	info.truck_no,
	info.actual_start_time,
	info.actual_end_time,
	date_format(from_unixtime(unix_timestamp(info.actual_start_time, 'yyyy-MM-dd HH:mm:ss') + shift_info.estimated_time * 60, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') estimate_end_time,
	info.actual_distance,
	cast(info.finish_dur_sec as bigint) finish_dur_sec,
	info.ts,
	info.dt
from (
	select
		data.id,
		data.shift_id,
		data.line_id,
		data.start_org_id,
		data.start_org_name,
		data.end_org_id,
		data.end_org_name,
		data.order_num,
		data.driver1_emp_id,
		data.driver1_name,
		data.driver2_emp_id,
		data.driver2_name,
		data.truck_id,
		data.truck_no,
		data.actual_start_time,
		data.actual_end_time,
		data.actual_distance,
		unix_timestamp(data.actual_end_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(data.actual_start_time, 'yyyy-MM-dd HH:mm:ss') finish_dur_sec,
		ts,
		date_format(data.actual_end_time, 'yyyy-MM-dd') dt
	from ods_transport_task_inc
	where dt = '$2' and data.is_deleted = '0'
		and data.actual_end_time is not null 
		and type = 'bootstrap-insert'
) info
left join (
	select
		id,
		estimated_time
	from dim_shift_full
	where dt = '$2'
)  shift_info
on info.shift_id = shift_info.id;
"



# 中转域入库事务事实表(dwd_bound_inbound_inc)
dwd_bound_inbound_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_bound_inbound_inc partition (dt)
select
	data.id,
	data.order_id,
	data.org_id,
	data.inbound_time,
	data.inbound_emp_id,
	date_format(data.inbound_time, 'yyyy-MM-dd') dt
from ods_order_org_bound_inc
where dt = '$2'
	and data.is_deleted = '0'
	and type = 'bootstrap-insert';
"





# 中转域分拣事务事实表(dwd_bound_sort_inc)
dwd_bound_sort_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_bound_sort_inc partition (dt)
select
	data.id,
	data.order_id,
	data.org_id,
	data.sort_time,
	data.sorter_emp_id,
	date_format(data.sort_time, 'yyyy-MM-dd') dt
from ods_order_org_bound_inc
where dt = '$2' 
	and data.is_deleted = '0'
	and type = 'bootstrap-insert'
	and data.sort_time is not null;
"





# 中转域出库事务事实表(dwd_bound_outbound_inc)
dwd_bound_outbound_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_bound_outbound_inc partition (dt)
select
	data.id,
	data.order_id,
	data.org_id,
	data.outbound_time,
	data.outbound_emp_id,
	date_format(data.outbound_time, 'yyyy-MM-dd') dt
from ods_order_org_bound_inc
where dt = '$2' 
	and data.is_deleted = '0'
	and type = 'bootstrap-insert'
	and data.outbound_time is not null;
"

exec_func(){
	echo "use tms;${!1}"
	$HIVE_PATH/hive -e "use tms;${!1}"
}


case $1 in
	"dwd_trade")
		$HIVE_PATH/hive -e "use tms;${dwd_trade_order_detail_inc_sql};${dwd_trade_pay_suc_detail_inc_sql};${dwd_trade_cancel_detail_inc_sql};${dwd_trade_order_process_inc_sql};"
		;;
	"dwd_trans")
		$HIVE_PATH/hive -e "use tms;${dwd_trans_receive_detail_inc_sql};${dwd_trans_dispatch_detail_inc_sql};${dwd_trans_bound_finish_detail_inc_sql};${dwd_trans_deliver_suc_detail_inc_sql};${dwd_trans_sign_detail_inc_sql};${dwd_trans_trans_finish_inc_sql};"
		;;
	"dwd_bound")
		$HIVE_PATH/hive -e "use tms;${dwd_bound_inbound_inc_sql};${dwd_bound_sort_inc_sql};${dwd_bound_outbound_inc_sql};"
		;;
	dwd_trade_order_detail_inc | dwd_trade_pay_suc_detail_inc| dwd_trade_cancel_detail_inc | dwd_trade_order_process_inc | dwd_trans_receive_detail_inc | dwd_trans_dispatch_detail_inc | dwd_trans_bound_finish_detail_inc| dwd_trans_deliver_suc_detail_inc | dwd_trans_sign_detail_inc | dwd_trans_trans_finish_inc | dwd_bound_inbound_inc | dwd_bound_sort_inc | dwd_bound_outbound_inc_sql)
		sql="${1}_sql"
		exec_func $sql
		;;
esac