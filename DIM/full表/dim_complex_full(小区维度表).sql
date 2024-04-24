drop table if exists dim_complex_full;
create external table dim_complex_full(
	`id` bigint comment '小区ID',
	`complex_name` string comment '小区名称',
	`courier_emp_ids` array<string> comment '负责快递员IDS',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`district_id` bigint comment '区（县）ID',
	`district_name` string comment '区（县）名称'
) comment '小区维度表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dim/dim_complex_full'
tblproperties ('orc.compression' = 'snappy');


-- 数据转载
-- 首日装载
insert overwrite table dim_complex_full partition (dt='2024-01-07')
select
	com.id,
	complex_name,
	cour_com.courier_emp_ids,
	province_id,
	priv_reg.province_name,
	city_id,
	city_reg.city_name,
	district_id,
	district_name
from (
	select
		id,
		complex_name,
		province_id,
		city_id,
		district_id,
		district_name
	from ods_base_complex_full
	where dt = '2024-01-07' and is_deleted = '0'
) com
inner join (
	select
		id,
		name province_name
	from ods_base_region_info_full
	where dt = '2024-01-07' and is_deleted = '0'
) priv_reg
on com.province_id = priv_reg.id 
inner join (
	select
		id,
		name city_name
	from ods_base_region_info_full
	where dt = '2024-01-07' and is_deleted = '0'
) city_reg
on com.city_id = city_reg.id
left join (
	select
		complex_id,
		collect_set(cast(courier_emp_id as string)) courier_emp_ids
	from ods_express_courier_complex_full
	where dt = '2024-01-07' and is_deleted = '0'
	group by complex_id
) cour_com
on com.id = cour_com.complex_id;