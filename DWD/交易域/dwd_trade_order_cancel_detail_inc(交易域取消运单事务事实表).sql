drop table if exists dwd_trade_order_cancel_detail_inc;
create external table dwd_trade_order_cancel_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`cancel_time` string COMMENT '取消时间',
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
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '交易域取消运单事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_order_cancel_detail_inc'
tblproperties ('orc.compression' = 'snappy');





-- 数据装载
-- 首日装载
set hive.exec.dynamc.partition.mode=nostrict;
inser overwrite table dwd_trade_order_cancel_detail_inc partition (dt)
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
		data.cargo_num,
		data.amount,
		data.estimate_arrive_time,
		data.distance,
		data.update_time cancel_time
	from ods_order_info_inc
	where dt = '2024-01-07' 
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



-- 每日装载
with cancel_info as (
	-- 每日取消运单操作的数据
	select
		order_info.id,
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
			data.update_time cancel_time,
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
			ts
		from ods_order_info_inc
		where dt = '2024-01-08'
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