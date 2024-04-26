drop table if exists dwd_trade_pay_suc_detail_inc;
create external table dwd_trade_pay_suc_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`payment_time` string COMMENT '支付时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '交易域支付成功事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_pay_suc_detail_inc'
tblproperties ('orc.compression' = 'snappy');




-- 数据装载
-- 首日装载
set hive.exec.dynamic.partition.mode=nostrict;
inser overwrite table dwd_trade_pay_suc_detail_inc partition (dt)
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
	where dt = '2024-01-07' and data.is_deleted = '0' and type = 'bootstrap-insert'
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
	where dt = '2024-01-07' 
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
	select
		id payment_type_id,
		name payment_type_name
	from ods_base_dic_full
	where dt = '2024-01-07' and is_deleted = '0'
) payment_type_dic
on order_info.payment_type = payment_type_dic.payment_type_id;





-- 每日装载
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
		where dt = '2024-01-08' 
			and data.is_deleted = '0' 
			and type = 'update' 
			and data.status = '60020'
			and old["status"] = '60010'
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
		where dt = '2024-01-08' 
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
		where dt = '2024-01-08' 
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
insert overwrite table dwd_trade_pay_suc_detail_inc partition (dt='2024-01-08')
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