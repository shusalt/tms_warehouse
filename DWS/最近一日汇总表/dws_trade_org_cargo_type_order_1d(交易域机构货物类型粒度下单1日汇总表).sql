drop table if exists dws_trade_org_cargo_type_order_1d;
create external table dws_trade_org_cargo_type_order_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`cargo_type` string comment '货物类型',
	`cargo_type_name` string comment '货物类型名称',
	`order_count` bigint comment '下单数',
	`order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_1d'
tblproperties ('orc.compression' = 'snappy');



-- 数据装载
-- 首日转载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dws_trade_org_cargo_type_order_1d partition (dt)
select
	org_id,
	org_name,
	city_id,
	city_name,
	cargo_type,
	cargo_type_name,
	count(order_id) order_count,
	sum(amount) order_amount,
	dt
from (
	select
		cargo_type,
		cargo_type_name,
		city_id,
		city_name,
		org_id,
		org_name,
		order_id,
		amount,
		dt
	from (
		select
			order_id,
			cargo_type,
			cargo_type_name,
			sender_city_id city_id,
			sender_district_id,
			max(amount) amount,
			dt
		from dwd_trade_order_detail_inc
		group by 
			order_id,
			cargo_type,
			cargo_type_name,
			sender_city_id,
			sender_district_id,
			dt
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
	org_name,
	dt





-- 每日装载
insert overwrite table dws_trade_org_cargo_type_order_1d partition (dt='2024-01-08')
select
	org_id,
	org_name,
	city_id,
	city_name,
	cargo_type,
	cargo_type_name,
	collect_set(order_id) order_id_coll,
	count(order_id) order_ct,
	sum(amount) sum_amount
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
			where dt = '2024-01-08'
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
	org_name 