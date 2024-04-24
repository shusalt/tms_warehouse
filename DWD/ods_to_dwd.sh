#!/bin/bash

APP=tms
HIVE_PATH=/usr/hive-2.1/bin

if [ $# -lt 1 ]; then
	echo "必须传入all/表名..."
	exit
fi

[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' + %F)


# 交易域订单明细事务事实表(dwd_trade_order_detail_inc)
dwd_trade_order_detail_inc_sql="
insert overwrite table dwd_trade_order_detail_inc partition (dt='$2')
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
	ts
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
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'insert'
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
	where dt = '$2' 
		and data.is_deleted = '0' 
		and type = 'insert'
) order_info
on order_cargo.order_id = order_info.order_id
left join (
	-- 货物类型名
	select
		id cargo_type_id,
		name cargo_type_name
	from ods_base_dic_full
	where dt = '2024-01-07' and is_deleted = '0'
) cargo_type_dic
on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
left join (
	-- 状态类型名
	select
		id status_dic_id,
		name status_name
	from ods_base_dic_full
	where dt = '2024-01-07' and is_deleted = '0'
) status_dic
on order_info.status = status_dic.status_dic_id
left join (
	-- 收集类型名
	select
		id collect_type_id,
		name collect_type_name
	from ods_base_dic_full
	where dt = '2024-01-07' and is_deleted = '0'
) collect_type_dic
on order_info.collect_type = collect_type_dic.collect_type_id;
"




# 交易域支付成功事务事实表(dwd_trade_pay_suc_detail_inc)
dwd_trade_pay_suc_detail_inc_sql="
with pay_info as (
	-- 每日支付成功操作的数据
	select
		order_info.id,
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
		ts
	from (
		select
			data.id,
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
			data.update_time payment_time,
			ts
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'update' 
			and data.status not in ('60010', '60999')
	) order_info
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
	left join (
		select
			id payment_type_id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.payment_type_id
),
order_info_info as (
	-- 历史累积的未完成运单数据（状态为："已下单待支付"）
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		payment_type,
		payment_type_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance
	from dwd_trade_order_process_inc
	where dt = '9999-31-12'
		and status = '60010'
	union
	-- 当日新增的运单数据
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
		'' payment_type,
		'' paymnet_type_name,
		order_info.cargo_num,
		order_info.amount,
		order_info.estimate_arrive_time,
		order_info.distance
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
)
insert overwrite table dwd_trade_pay_suc_detail_inc partition (dt='$2')
select
	order_info_info.id,
	order_info_info.order_id,
	order_info_info.cargo_type,
	order_info_info.cargo_type_name,
	order_info_info.volumn_length,
	order_info_info.volumn_width,
	order_info_info.volumn_height,
	order_info_info.weight,
	pay_info.payment_time,
	pay_info.order_no,
	pay_info.status,
	pay_info.status_name,
	pay_info.collect_type,
	pay_info.collect_type_name,
	pay_info.user_id,
	pay_info.receiver_complex_id,
	pay_info.receiver_province_id,
	pay_info.receiver_city_id,
	pay_info.receiver_district_id,
	pay_info.receiver_name,
	pay_info.sender_complex_id,
	pay_info.sender_province_id,
	pay_info.sender_city_id,
	pay_info.sender_district_id,
	pay_info.sender_name,
	pay_info.payment_type,
	pay_info.payment_type_name,
	pay_info.cargo_num,
	pay_info.amount,
	pay_info.estimate_arrive_time,
	pay_info.distance,
	pay_info.ts
from pay_info
join order_info_info
on pay_info.id = order_info_info.order_id;
"






# 交易域取消运单事务事实表(dwd_trade_order_cancel_detail_inc)
dwd_trade_order_cancel_detail_inc_sql="
with cancel_info as (
	-- 每日取消运单操作的数据
	select
		order_info.id
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
		order_info.ts
	from (
		select
			data.id
			data.cancel_time,
			data.order_no,
			data.status,
			data.collect_type,
			data.user_id,
			data.receiver_complex_id,
			data.receiver_province_id,
			data.receiver_city_id,
			data.receiver_district_id,
			data.receiver_name,
			data.sender_complex_id,
			data.sender_province_id,
			data.sender_city_id,
			data.sender_district_id,
			data.sender_name,
			data.cargo_num,
			data.amount,
			data.estimate_arrive_time,
			data.distance,
			data.ts
		from ods_order_info_inc
		where dt = '$2'
			and data.is_deleted = '0'
			and type = 'update'
			and data.status = '60999'
	) order_info
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id;
),
order_info_info as (
	-- 历史累积的未完成运单数据
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance
	from dwd_trade_process_inc
	where dt = '9999-12-31'
	union
	-- 当日新增运单数据
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
		order_info.distance
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
)
insert overwrite table dwd_trade_order_cancel_detail_inc
select
	order_info_info.id,
	order_info_info.order_id,
	order_info_info.cargo_type,
	order_info_info.cargo_type_name,
	order_info_info.volumn_length,
	order_info_info.volumn_width,
	order_info_info.volumn_height,
	order_info_info.weight,
	cancel_info.cancel_time,
	cancel_info.order_no,
	cancel_info.status,
	cancel_info.status_name,
	cancel_info.collect_type,
	cancel_info.collect_type_name,
	cancel_info.user_id,
	cancel_info.receiver_complex_id,
	cancel_info.receiver_province_id,
	cancel_info.receiver_city_id,
	cancel_info.receiver_district_id,
	cancel_info.receiver_name,
	cancel_info.sender_complex_id,
	cancel_info.sender_province_id,
	cancel_info.sender_city_id,
	cancel_info.sender_district_id,
	cancel_info.sender_name,
	cancel_info.cargo_num,
	cancel_info.amount,
	cancel_info.estimate_arrive_time,
	cancel_info.distance,
	cancel_info.ts
from cancel_info
join order_info_info
on cancel_info.id = order_info_info.order_id;
"



# 交易域运单累积快照事实表(dwd_trade_order_process_inc)
dwd_trade_order_process_inc_sql="
set hive.exec.dynamic.partition.mode=nostrict;
with order_info_info as (
	-- 历史累积未完成的order数据
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		payment_type,
		payment_type_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance,
		ts,
		start_date,
		end_date
	from dwd_trade_order_process_inc
	where dt = '9999-12-31'
	union
	-- 当日新增的order数据
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
		date_format(order_time, 'yyyy-MM-dd') start_date,
		'9999-12-31' end_date
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0'
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
	left join (
		-- 支付类型
		select
			id payment_type_id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.payment_type_id
),
update_info as (
	select
		order_info.id,
		order_info.status,
		status_dic.status_name,
		order_info.payment_type,
		payment_type_dic.payment_type_name
	from (
		select
			id,
			status,
			payment_type
		from (
			select
				data.id,
				data.status,
				data.payment_type,
				row_number() over(partition by data.id order by ts desc) rn
			from ods_order_info_inc
			where dt = '$2' 
				and type = 'update' 				
		) tmp1
		where rn = 1		
	) order_info
	left join (
		select
			id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.id
	left join (
		select
			id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.id
)
insert overwrite table dwd_trade_order_process_inc partition(dt)
select
	id,
	order_id,
	cargo_type,
	cargo_type_name,
	volumn_length,
	volumn_width,
	volumn_height,
	weight,
	order_time,
	order_no,
	status,
	status_name,
	collect_type,
	collect_type_name,
	user_id,
	receiver_complex_id,
	receiver_province_id,
	receiver_city_id,
	receiver_district_id,
	receiver_name,
	sender_complex_id,
	sender_province_id,
	sender_city_id,
	sender_district_id,
	sender_name,
	payment_type,
	payment_type_name,
	cargo_num,
	amount,
	estimate_arrive_time,
	distance,
	ts,
	start_date,
	if(status = '60080' or status = '60999', '2024-01-08', end_date) end_date,
	if(status = '60080' or status = '60999', '2024-01-08', end_date) dt
from (
	select
		order_info_info.id,
		order_info_info.order_id,
		order_info_info.cargo_type,
		order_info_info.cargo_type_name,
		order_info_info.volumn_length,
		order_info_info.volumn_width,
		order_info_info.volumn_height,
		order_info_info.weight,
		order_info_info.order_time,
		order_info_info.order_no,
		order_info_info.status status1,
		update_info.status status2,
		if(update_info.status is not null, update_info.status, order_info_info.status) status,
		order_info_info.status_name status1_name,
		update_info.status_name status2_name,
		if(update_info.status_name is not null, update_info.status_name, order_info_info.status_name) status_name,
		order_info_info.collect_type,
		order_info_info.collect_type_name,
		order_info_info.user_id,
		order_info_info.receiver_complex_id,
		order_info_info.receiver_province_id,
		order_info_info.receiver_city_id,
		order_info_info.receiver_district_id,
		order_info_info.receiver_name,
		order_info_info.sender_complex_id,
		order_info_info.sender_province_id,
		order_info_info.sender_city_id,
		order_info_info.sender_district_id,
		order_info_info.sender_name,
		order_info_info.payment_type payment_type1,
		update_info.payment_type payment_type2,
		if(update_info.payment_type is not null, update_info.payment_type, order_info_info.payment_type) payment_type,
		order_info_info.payment_type_name payment_type_name1,
		update_info.payment_type_name payment_type_name2,
		if(update_info.payment_type_name is not null, update_info.payment_type_name, order_info_info.payment_type_name) payment_type_name,
		order_info_info.cargo_num,
		order_info_info.amount,
		order_info_info.estimate_arrive_time,
		order_info_info.distance,
		order_info_info.ts,
		order_info_info.start_date,
		order_info_info.end_date
	from order_info_info
	left join update_info
	on order_info_info.order_id = update_info.id
) tmp2;
"



# 物流域揽收事务事实表(dwd_trans_receive_detail_inc)
dwd_trans_receive_detail_inc_sql="
with order_info_info as (
	-- 历史累积的未完成运单数据
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		payment_type,
		payment_type_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance
	from dwd_trade_order_process_inc
	where dt = '9999-12-31'
	union
	-- 当日新增运单数据
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
		'' payment_type,
		'' payment_type_name, 
		order_info.cargo_num,
		order_info.amount,
		order_info.estimate_arrive_time,
		order_info.distance
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
),
receive_inc as (
	-- 每日揽收操作的数据
	select
		order_info.id,
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
		ts
	from (
		select
			data.id,
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
			data.update_time receive_time,
			ts
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'update' 
			and data.status not in ('60010', '60020', '60999')
	) order_info
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
	left join (
		select
			id payment_type_id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.payment_type_id
)
insert overwrite table dwd_trans_receive_detail_inc partition (dt='$2')
select
	order_info_info.id,
	order_info_info.order_id,
	order_info_info.cargo_type,
	order_info_info.cargo_type_name,
	order_info_info.volumn_length,
	order_info_info.volumn_width,
	order_info_info.volumn_height,
	order_info_info.weight,
	receive_inc.receive_time,
	receive_inc.order_no,
	receive_inc.status,
	receive_inc.status_name,
	receive_inc.collect_type,
	receive_inc.collect_type_name,
	receive_inc.user_id,
	receive_inc.receiver_complex_id,
	receive_inc.receiver_province_id,
	receive_inc.receiver_city_id,
	receive_inc.receiver_district_id,
	receive_inc.receiver_name,
	receive_inc.sender_complex_id,
	receive_inc.sender_province_id,
	receive_inc.sender_city_id,
	receive_inc.sender_district_id,
	receive_inc.sender_name,
	receive_inc.payment_type,
	receive_inc.payment_type_name,
	receive_inc.cargo_num,
	receive_inc.amount,
	receive_inc.estimate_arrive_time,
	receive_inc.distance,
	receive_inc.ts
from receive_inc
join order_info_info
on receive_inc.id = order_info_info.order_id;
"





# 物流域发单事务事实表(dwd_trans_dispatch_detail_inc)
dwd_trans_dispatch_detail_inc_sql="
with order_info_info as(
	-- 历史累积的未完成运单数据
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		payment_type,
		payment_type_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance
	from dwd_trade_order_process_inc
	where dt = '9999-12-31'
	union
	-- 当日新增运单数据
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
		'' payment_type,
		'' payment_type_name, 
		order_info.cargo_num,
		order_info.amount,
		order_info.estimate_arrive_time,
		order_info.distance
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
),
dispatch_inc as (
	-- 每日发单操作的数据
	select
		order_info.id,
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
		ts
	from (
		select
			data.id,
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
			data.update_time dispatch_time,
			ts
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'update' 
			and data.status not in ('60010', '60020', '60030', '60040', '60999')
	) order_info
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
	left join (
		select
			id payment_type_id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.payment_type_id
)
insert overwrite table dwd_trans_dispatch_detail_inc partition(dt='$2')
select
	order_info_info.id,
	order_info_info.order_id,
	order_info_info.cargo_type,
	order_info_info.cargo_type_name,
	order_info_info.volumn_length,
	order_info_info.volumn_width,
	order_info_info.volumn_height,
	order_info_info.weight,
	dispatch_inc.dispatch_time,
	dispatch_inc.order_no,
	dispatch_inc.status,
	dispatch_inc.status_name,
	dispatch_inc.collect_type,
	dispatch_inc.collect_type_name,
	dispatch_inc.user_id,
	dispatch_inc.receiver_complex_id,
	dispatch_inc.receiver_province_id,
	dispatch_inc.receiver_city_id,
	dispatch_inc.receiver_district_id,
	dispatch_inc.receiver_name,
	dispatch_inc.sender_complex_id,
	dispatch_inc.sender_province_id,
	dispatch_inc.sender_city_id,
	dispatch_inc.sender_district_id,
	dispatch_inc.sender_name,
	dispatch_inc.payment_type,
	dispatch_inc.payment_type_name,
	dispatch_inc.cargo_num,
	dispatch_inc.amount,
	dispatch_inc.estimate_arrive_time,
	dispatch_inc.distance,
	dispatch_inc.ts
from dispatch_inc
join order_info_info
on dispatch_inc.id = order_info_info.order_id;
"





# 物流域转运完成事务事实表(dwd_trans_bound_finish_detail_inc)
dwd_trans_bound_finish_detail_inc_sql="
with order_info_info as (
	-- 历史累积的未完成运单数据
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		payment_type,
		payment_type_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance
	from dwd_trade_order_process_inc
	where dt = '9999-12-31'
	union
	-- 当日新增运单数据
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
		'' payment_type,
		'' payment_type_name, 
		order_info.cargo_num,
		order_info.amount,
		order_info.estimate_arrive_time,
		order_info.distance
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
),
bound_finish_inc as (
	-- 每日转运完成操作的数据
	select
		order_info.id,
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
		ts
	from (
		select
			data.id,
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
			data.update_time bound_finish_time,
			ts
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'update' 
			and data.status not in ('60010', '60020', '60030', '60040', '60050', '60999')
	) order_info
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
	left join (
		select
			id payment_type_id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.payment_type_id
)
insert overwrite table dwd_trans_bound_finish_detail_inc partition (dt='$2')
select
	order_info_info.id,
	order_info_info.order_id,
	order_info_info.cargo_type,
	order_info_info.cargo_type_name,
	order_info_info.volumn_length,
	order_info_info.volumn_width,
	order_info_info.volumn_height,
	order_info_info.weight,
	bound_finish_inc.bound_finish_time,
	bound_finish_inc.order_no,
	bound_finish_inc.status,
	bound_finish_inc.status_name,
	bound_finish_inc.collect_type,
	bound_finish_inc.collect_type_name,
	bound_finish_inc.user_id,
	bound_finish_inc.receiver_complex_id,
	bound_finish_inc.receiver_province_id,
	bound_finish_inc.receiver_city_id,
	bound_finish_inc.receiver_district_id,
	bound_finish_inc.receiver_name,
	bound_finish_inc.sender_complex_id,
	bound_finish_inc.sender_province_id,
	bound_finish_inc.sender_city_id,
	bound_finish_inc.sender_district_id,
	bound_finish_inc.sender_name,
	bound_finish_inc.payment_type,
	bound_finish_inc.payment_type_name,
	bound_finish_inc.cargo_num,
	bound_finish_inc.amount,
	bound_finish_inc.estimate_arrive_time,
	bound_finish_inc.distance,
	bound_finish_inc.ts
from bound_finish_inc
join order_info_info
on bound_finish_inc.id = order_info_info.order_id;
"




# 物流域派生完成事务事实表(dwd_trans_deliver_suc_detail_inc)
dwd_trans_deliver_suc_detail_inc_sql="
with order_info_info as (
	-- 历史累积的未完成运单数据
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		payment_type,
		payment_type_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance
	from dwd_trade_order_process_inc
	where dt = '9999-12-31'
	union
	-- 当日新增运单数据
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
		'' payment_type,
		'' payment_type_name, 
		order_info.cargo_num,
		order_info.amount,
		order_info.estimate_arrive_time,
		order_info.distance
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
),
deliver_suc_inc as (
	-- 每日派送完成操作的数据
	select
		order_info.id,
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
		ts
	from (
		select
			data.id,
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
			data.update_time deliver_suc_time,
			ts
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'update' 
			and data.status not in ('60010', '60020', '60030', '60040', '60050', '60060', '60999')
	) order_info
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
	left join (
		select
			id payment_type_id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.payment_type_id
)
insert overwrite table dwd_trans_deliver_suc_detail_inc partition (dt='$2')
select
	order_info_info.id,
	order_info_info.order_id,
	order_info_info.cargo_type,
	order_info_info.cargo_type_name,
	order_info_info.volumn_length,
	order_info_info.volumn_width,
	order_info_info.volumn_height,
	order_info_info.weight,
	deliver_suc_inc.deliver_suc_time,
	deliver_suc_inc.order_no,
	deliver_suc_inc.status,
	deliver_suc_inc.status_name,
	deliver_suc_inc.collect_type,
	deliver_suc_inc.collect_type_name,
	deliver_suc_inc.user_id,
	deliver_suc_inc.receiver_complex_id,
	deliver_suc_inc.receiver_province_id,
	deliver_suc_inc.receiver_city_id,
	deliver_suc_inc.receiver_district_id,
	deliver_suc_inc.receiver_name,
	deliver_suc_inc.sender_complex_id,
	deliver_suc_inc.sender_province_id,
	deliver_suc_inc.sender_city_id,
	deliver_suc_inc.sender_district_id,
	deliver_suc_inc.sender_name,
	deliver_suc_inc.payment_type,
	deliver_suc_inc.payment_type_name,
	deliver_suc_inc.cargo_num,
	deliver_suc_inc.amount,
	deliver_suc_inc.estimate_arrive_time,
	deliver_suc_inc.distance,
	deliver_suc_inc.ts
from deliver_suc_inc
join order_info_info
on deliver_suc_inc.id = order_info_info.order_id;
"




# 物流域签收事务事实表(dwd_trans_sign_detail_inc)
dwd_trans_sign_detail_inc_sql="
with order_info_info as (
	-- 历史累积的未完成运单数据
	select
		id,
		order_id,
		cargo_type,
		cargo_type_name,
		volumn_length,
		volumn_width,
		volumn_height,
		weight,
		order_time,
		order_no,
		status,
		status_name,
		collect_type,
		collect_type_name,
		user_id,
		receiver_complex_id,
		receiver_province_id,
		receiver_city_id,
		receiver_district_id,
		receiver_name,
		sender_complex_id,
		sender_province_id,
		sender_city_id,
		sender_district_id,
		sender_name,
		payment_type,
		payment_type_name,
		cargo_num,
		amount,
		estimate_arrive_time,
		distance
	from dwd_trade_order_process_inc
	where dt = '9999-12-31'
	union
	-- 当日新增运单数据
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
		'' payment_type,
		'' payment_type_name, 
		order_info.cargo_num,
		order_info.amount,
		order_info.estimate_arrive_time,
		order_info.distance
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
			and type = 'insert'
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
			data.distance
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'insert'
	) order_info
	on order_cargo.order_id = order_info.order_id
	left join (
		-- 货物类型名
		select
			id cargo_type_id,
			name cargo_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) cargo_type_dic
	on order_cargo.cargo_type = cargo_type_dic.cargo_type_id
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
),
sign_inc as (
	-- 每日签收完成操作的数据
	select
		order_info.id,
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
		ts
	from (
		select
			data.id,
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
			data.update_time sign_time,
			ts
		from ods_order_info_inc
		where dt = '$2' 
			and data.is_deleted = '0' 
			and type = 'update' 
			and data.status not in ('60010', '60020', '60030', '60040', '60050', '60060', '60070', '60999')
	) order_info
	left join (
		-- 状态类型名
		select
			id status_dic_id,
			name status_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) status_dic
	on order_info.status = status_dic.status_dic_id
	left join (
		-- 收集类型名
		select
			id collect_type_id,
			name collect_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) collect_type_dic
	on order_info.collect_type = collect_type_dic.collect_type_id
	left join (
		select
			id payment_type_id,
			name payment_type_name
		from ods_base_dic_full
		where dt = '2024-01-07' and is_deleted = '0'
	) payment_type_dic
	on order_info.payment_type = payment_type_dic.payment_type_id
)
insert overwrite table dwd_trans_sign_detail_inc partition (dt='$2')
select
	order_info_info.id,
	order_info_info.order_id,
	order_info_info.cargo_type,
	order_info_info.cargo_type_name,
	order_info_info.volumn_length,
	order_info_info.volumn_width,
	order_info_info.volumn_height,
	order_info_info.weight,
	sign_inc.sign_time,
	sign_inc.order_no,
	sign_inc.status,
	sign_inc.status_name,
	sign_inc.collect_type,
	sign_inc.collect_type_name,
	sign_inc.user_id,
	sign_inc.receiver_complex_id,
	sign_inc.receiver_province_id,
	sign_inc.receiver_city_id,
	sign_inc.receiver_district_id,
	sign_inc.receiver_name,
	sign_inc.sender_complex_id,
	sign_inc.sender_province_id,
	sign_inc.sender_city_id,
	sign_inc.sender_district_id,
	sign_inc.sender_name,
	sign_inc.payment_type,
	sign_inc.payment_type_name,
	sign_inc.cargo_num,
	sign_inc.amount,
	sign_inc.estimate_arrive_time,
	sign_inc.distance,
	sign_inc.ts
from sign_inc
join order_info_info
on sign_inc.id = order_info_info.order_id;
"




# 物流域运输完成事务事实表(dwd_trans_trans_finish_inc)
dwd_trans_trans_finish_inc_sql="
insert overwrite table dwd_trans_trans_finish_inc partition (dt='$2')
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
	info.ts
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
	where dt = '$2'
		-- 提取变化的数
		and type = 'update'
		-- actual_end_time不为空，表示两个机构之间的一次运输已完成
		and data.actual_end_time is not null
) info
left join (
	select
		id,
		estimated_time
	from dim_shift_full
	where dt = '2024-01-07'
)  shift_info
on info.shift_id = shift_info.id;
"



# 中转入库事务事实表(dwd_bound_inbound_inc)
dwd_bound_inbound_inc_sql="
insert overwrite table dwd_bound_inbound_inc partition (dt='$2')
select
	data.id,
	data.order_id,
	data.org_id,
	data.inbound_time,
	data.inbound_emp_id
from ods_order_org_bound_inc
where dt = '2024-01-08' 
	and type = 'insert'
	and data.is_deleted = '0';
"




# 中转域分拣事务事实表(dwd_bound_sort_inc)
dwd_bound_sort_inc_sql="
insert overwrite table dwd_bound_sort_inc partition (dt='$2')
select
	data.id,
	data.order_id,
	data.org_id,
	data.sort_time,
	data.sorter_emp_id
from ods_order_org_bound_inc
lateral view explode(map_keys(old)) explode_old_key as old_key
where old_key = 'sort_time'	
	and dt = '2024-01-08' 
	and type = 'update'
	and data.is_deleted = '0'
	and data.sorter_emp_id is not null
	and old['sorter_emp_id'] is null;
"




# 中转域出库事务事实表(dwd_bound_outbound_inc)
dwd_bound_outbound_inc_sql="
insert overwrite table dwd_bound_outbound_inc partition (dt='$2')
select
	data.id,
	data.order_id,
	data.org_id,
	data.outbound_time,
	data.outbound_emp_id
from ods_order_org_bound_inc
lateral view explode(map_keys(old)) explode_old_key as old_key
where old_key = 'outbound_time'	
	and dt = '2024-01-08' 
	and type = 'update'
	and data.is_deleted = '0'
	and data.outbound_time is not null
	and old['outbound_time'] is null;
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