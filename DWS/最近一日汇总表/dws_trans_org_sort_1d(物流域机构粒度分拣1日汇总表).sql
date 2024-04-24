drop table if exists dws_trans_org_sort_1d;
create external table dws_trans_org_sort_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_sort_1d'
tblproperties ('orc.compression' = 'snappy');




-- 首日装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dws_trans_org_sort_1d partition (dt)
select
	org_id,
	org_name,
	city_id,
	city_name,
	province_id,
	province_name,
	count(id) sort_count,
	dt
from (
	select
		sort_info.org_id,
		org_name,
		if(org_level=1, city_for_level1.id, province_for_level1.id) city_id,
		if(org_level=1, city_for_level1.name, province_for_level1.name) city_name,
		if(org_level=1, province_for_level1.id, province_for_level2.id) province_id,
		if(org_level=1, province_for_level1.name, province_for_level2.name) province_name,
		sort_info.id,
		dt
	from (
		select
			id,
			org_id,
			dt
		from dwd_bound_sort_inc
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
	province_name,
	dt



-- 每日装载
insert overwrite table dws_trans_org_sort_1d partition (dt='2024-01-08')
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
			org_id,
			dt
		from dwd_bound_sort_inc
		where dt = '2024-01-08'
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
	province_name