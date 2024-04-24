drop table if exists dws_trans_org_deliver_suc_1d;
create external table dws_trans_org_deliver_suc_1d(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_deliver_suc_1d'
tblproperties ('orc.compression' = 'snappy');



-- 首日装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dws_trans_org_deliver_suc_1d partition (dt)
select
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	count(order_id) order_count,
	dt	
from (
	select
		org_id,
		org_name,
		province_id,
		province_name,
		city_id,
		city_name,
		order_id,
		dt
	from (
		select
			receiver_province_id province_id,
			receiver_city_id city_id,
			receiver_district_id district_id,
			dt,
			max(order_id) order_id
		from dwd_trans_deliver_suc_detail_inc
		group by
			receiver_province_id,
			receiver_city_id,
			receiver_district_id,
			dt
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
	city_name,
	dt





-- 每日装载
insert overwrite table dws_trans_org_deliver_suc_1d partition (dt='2024-01-08')
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
			dt,
			max(order_id) order_id
		from dwd_trans_deliver_suc_detail_inc
		where dt = '2024-01-08'
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