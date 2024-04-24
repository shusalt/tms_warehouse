drop table if exists dws_trans_org_receive_1d;
create external table dws_trans_org_receive_1d(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`order_count` bigint comment '揽收次数',
	`order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收1日汇总表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_receive_1d'
tblproperties ('orc.compression' = 'snappy');



-- 首日装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dws_trans_org_receive_1d partition (dt)
select
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	count(order_id) order_count,
	sum(amount) order_amount,
	dt
from (
	select
		order_id,
		sender_province_id province_id,
		province_name,
		sender_city_id city_id,
		city_name,
		org_id,
		org_name,
		amount,
		dt
	from (
		select
			order_id,
			sender_province_id,
			sender_city_id,
			sender_district_id,
			max(amount) amount,
			dt
		from dwd_trans_receive_detail_inc
		group by
			order_id,
			sender_province_id,
			sender_city_id,
			sender_district_id,
			dt
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
	org_name,
	dt


-- 每日装载
insert overwrite table dws_trans_org_receive_1d partition (dt = '2024-01-08')
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
		where dt = '2024-01-08'
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
	org_name