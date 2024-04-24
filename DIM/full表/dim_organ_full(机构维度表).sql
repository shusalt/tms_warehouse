drop table if exists dim_organ_full
create external table dim_organ_full(
	`id` bigint COMMENT '机构ID',
	`org_name` string COMMENT '机构名称',
	`org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
	`region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
	`region_name` string COMMENT '地区名称',
	`region_code` string COMMENT '地区编码（行政级别）',
	`org_parent_id` bigint COMMENT '父级机构ID',
	`org_parent_name` string COMMENT '父级机构名称'
) comment '机构维度表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dim/dim_organ_full'
tblproperties ('orc.compression'='snappy')



-- 数据装载
insert overwrite table dim_organ_full partition (dt='2024-01-07')
select
	org.id,
	org.org_name,
	org.org_level,
	org.region_id,
	reg.region_name,
	reg.region_code,
	org.org_parent_id,
	parent_org.org_parent_name
from (
	select
		id,
		org_name,
		org_level,
		region_id,
		org_parent_id,
	from ods_base_organ_full
	where dt = '2024-01-07' and is_deleted = '0'
) org
inner join (
	select
		id,
		org_name org_parent_name
	from ods_base_organ_full
	where dt = '2024-01-07' and is_deleted = '0'	
) parent_org
on org.org_parent_id = parent_org.id
inner join (
	select
		id,
		name region_name,
		dict_code region_code
	from ods_base_region_info_full
	where dt = '2024-01-07' and is_deleted = '0'
) reg
on org.region_id = reg.id;