drop table if exists ods_base_region_info_full;
create external table ods_base_region_info_full(
	`id` bigint COMMENT '地区ID',
	`parent_id` bigint COMMENT '父级地区ID',
	`name` string COMMENT '地区名称',
	`dict_code` string COMMENT '编码（行政级别）',
	`short_name` string COMMENT '简称',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` tinyint COMMENT '删除标记（0:不可用 1:可用）'
) comment '地区全量表'
partitioned by (`dt` string)
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_base_region_info_full';