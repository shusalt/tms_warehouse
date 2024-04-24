drop table if exists ods_truck_driver_full;
create external table ods_truck_driver_full(
	`id` bigint COMMENT '司机信息ID',
	`emp_id` bigint COMMENT '员工ID',
	`org_id` bigint COMMENT '所属机构ID',
	`team_id` bigint COMMENT '所属车队ID',
	`license_type` string COMMENT '准驾车型',
	`init_license_date` string COMMENT '初次领证日期',
	`expire_date` string COMMENT '有效截止日期',
	`license_no` string COMMENT '驾驶证号',
	`license_picture_url` string COMMENT '驾驶证图片链接',
	`is_enabled` tinyint COMMENT '状态 0：禁用 1：正常',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '司机信息全量表'
partitioned by (`dt` string comment '统计日期信息')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_truck_driver_full';