drop table if exists ods_order_cargo_inc;
create external table ods_order_cargo_inc(
	`type` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`order_id`:string,`cargo_type`:string,`volume_length`:bigint,`volume_width`:bigint,`volume_height`:bigint,`weight`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '运单明细增量表'
partitioned by (`dt` string comment '统计日期')
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_order_cargo_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

drop table if exists ods_order_info_inc;
create external table ods_order_info_inc(
	`type` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`order_no`:string,`status`:string,`collect_type`:string,`user_id`:bigint,`receiver_complex_id`:bigint,`receiver_province_id`:string,`receiver_city_id`:string,`receiver_district_id`:string,`receiver_address`:string,`receiver_name`:string,`sender_complex_id`:bigint,`sender_province_id`:string,`sender_city_id`:string,`sender_district_id`:string,`sender_name`:string,`payment_type`:string,`cargo_num`:bigint,`amount`:decimal(16,2),`estimate_arrive_time`:string,`distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值' 
) comment '运单增量表'
partitioned by (`dt` string)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_order_info_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec'); 

drop table if exists ods_order_org_bound_inc;
create external table ods_order_org_bound_inc(
	`type` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`order_id`:bigint,`org_id`:bigint,`status`:string,`inbound_time`:string,`inbound_emp_id`:bigint,`sort_time`:string,`sorter_emp_id`:bigint,`outbound_time`:string,`outbound_emp_id`:bigint,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '运输机构中转表增量'
partitioned by (`dt` string comment '统计日期')
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_order_org_bound_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

drop table if exists ods_transport_task_inc;
create external table ods_transport_task_inc(
	`type` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`shift_id`:bigint,`line_id`:bigint,`start_org_id`:bigint,`start_org_name`:string,`end_org_id`:bigint,`end_org_name`:string,`status`:string,`order_num`:bigint,`driver1_emp_id`:bigint,`driver1_name`:string,`driver2_emp_id`:bigint,`driver2_name`:string,`truck_id`:bigint,`truck_no`:string,`actual_start_time`:string,`actual_end_time`:string,`actual_distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '插入或者修改后的数据',
	`old` map<string, string>
) comment '运输任务增量表'
partitioned by (`dt` string comment '统计日期')
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_transport_task_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

drop table if exists ods_user_address_inc;
create external table ods_user_address_inc(
	`type` string comment '变动操作',
	`ts` string comment '变更时间戳',
	`data` struct<`id`:bigint,`user_id`:bigint,`phone`:string,`province_id`:bigint,`city_id`:bigint,`district_id`:bigint,`complex_id`:bigint,`address`:string,`is_default`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '用户地址增量表'
partitioned by (`dt` string comment '统计日期')
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_user_address_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');

drop table if exists ods_user_info_inc;
create external table ods_user_info_inc(
	`type` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`login_name`:string,`nick_name`:string,`passwd`:string,`real_name`:string,`phone_num`:string,`email`:string,`user_level`:string,`birthday`:string,`gender`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '用户信息增量表'
partitioned by (`dt` string comment '统计日期')
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_user_info_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');





-- 全量表
-- 全量表
drop table if exists ods_base_complex_full;
create external table ods_base_complex_full(
	`id` bigint comment '小区ID',
	`complex_name` string comment '小区名称',
	`province_id` bigint comment '省份ID',
	`city_id` bigint comment '城市ID',
	`district_id` bigint comment '区（县）ID',
	`district_name` string comment '区（县）名称',
	`create_time` string comment '创建时间',
	`update_time` string comment '更新时间',
	`is_deleted` string comment '是否删除'
) comment '小区全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_base_complex_full';

drop table if exists ods_base_dic_full;
create external table ods_base_dic_full(
    `id` bigint comment '编号（主键）',
    `parent_id` bigint comment '父级编号',
    `name` string comment '名称',
    `dict_code` string comment '编码',
    `create_time` string comment '创建时间',
    `update_time` string comment '更新时间',
    `is_deleted` string comment '是否删除'
) comment '字典全量表'
partitioned by (`dt` string)
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_base_dic_full';

drop table if exists ods_base_organ_full;
create external table ods_base_organ_full(
	`id` bigint COMMENT '机构ID',
	`org_name` string COMMENT '机构名称',
	`org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
	`region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
	`org_parent_id` bigint COMMENT '父级机构ID',
	`points` string COMMENT '多边形经纬度坐标集合',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '机构全量表'
partitioned by (`dt` string)
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_base_organ_full';

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

drop table if exists ods_employee_info_full;
create external table ods_employee_info_full(
	`id` bigint COMMENT '员工ID',
	`username` string COMMENT '用户名',
	`password` string COMMENT '密码',
	`real_name` string COMMENT '真实姓名',
	`id_card` string COMMENT '身份证号',
	`phone` string COMMENT '手机号',
	`birthday` string COMMENT '生日',
	`gender` string COMMENT '性别',
	`address` string COMMENT '地址',
	`employment_date` string COMMENT '入职日期',
	`graduation_date` string COMMENT '离职日期',
	`education` string COMMENT '学历',
	`position_type` string COMMENT '岗位类别',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '员工全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_employee_info_full';

drop table if exists ods_express_courier_complex_full;
create external table ods_express_courier_complex_full(
	`id` bigint COMMENT '主键ID',
	`courier_emp_id` bigint COMMENT '快递员ID',
	`complex_id` bigint COMMENT '小区ID',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）' 
) comment '快递员小区关联表全量'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_express_courier_complex_full';

drop table if exists ods_express_courier_full;
create external table ods_express_courier_full(
	`id` bigint COMMENT '快递员ID',
	`emp_id` bigint COMMENT '员工ID',
	`org_id` bigint COMMENT '所属机构ID',
	`working_phone` string COMMENT '工作电话',
	`express_type` string COMMENT '快递员类型（收货；发货）',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '快递员信息全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_express_courier_full';

drop table if exists ods_line_base_info_full;
create external table ods_line_base_info_full(
	`id` bigint COMMENT '线路ID',
	`name` string COMMENT '线路名称',
	`line_no` string COMMENT '线路编号',
	`line_level` string COMMENT '线路级别',
	`org_id` bigint COMMENT '所属机构',
	`transport_line_type_id` string COMMENT '线路类型',
	`start_org_id` bigint COMMENT '起始机构ID',
	`start_org_name` string COMMENT '起始机构名称',
	`end_org_id` bigint COMMENT '目标机构ID',
	`end_org_name` string COMMENT '目标机构名称',
	`pair_line_id` bigint COMMENT '配对线路ID',
	`distance` decimal(10,2) COMMENT '预估里程',
	`cost` decimal(10,2) COMMENT '实际里程',
	`estimated_time` bigint COMMENT '预计时间（分钟）',
	`status` string COMMENT '状态 0：禁用 1：正常',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '运输线路全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_line_base_info_full';

drop table if exists ods_line_base_shift_full;
create external table ods_line_base_shift_full(
	`id` bigint COMMENT '班次ID',
	`line_id` bigint COMMENT '线路ID',
	`start_time` string COMMENT '班次开始时间',
	`driver1_emp_id` bigint COMMENT '第一司机',
	`driver2_emp_id` bigint COMMENT '第二司机',
	`truck_id` bigint COMMENT '卡车',
	`pair_shift_id` bigint COMMENT '配对班次(同一辆车一去一回的另一班次)',
	`is_enabled` string COMMENT '状态 0：禁用 1：正常',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '班次全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_line_base_shift_full';
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

drop table if exists ods_truck_info_full;
create external table ods_truck_info_full(
	`id` bigint COMMENT '卡车ID',
	`team_id` bigint COMMENT '所属车队ID',
	`truck_no` string COMMENT '车牌号码',
	`truck_model_id` string COMMENT '型号',
	`device_gps_id` string COMMENT 'GPS设备ID',
	`engine_no` string COMMENT '发动机编码',
	`license_registration_date` string COMMENT '注册时间',
	`license_last_check_date` string COMMENT '最后年检日期',
	`license_expire_date` string COMMENT '失效日期',
	`picture_url` string COMMENT '图片链接',
	`is_enabled` int COMMENT '状态 0：禁用 1：正常',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '卡车信息全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_truck_info_full';

drop table if exists ods_truck_model_full;
create external table ods_truck_model_full(
	`id` bigint COMMENT '型号ID',
	`model_name` string COMMENT '型号名称',
	`model_type` string COMMENT '型号类型',
	`model_no` string COMMENT '型号编码',
	`brand` string COMMENT '品牌',
	`truck_weight` decimal(16,2) COMMENT '整车重量（吨）',
	`load_weight` decimal(16,2) COMMENT '额定载重（吨）',
	`total_weight` decimal(16,2) COMMENT '总质量（吨）',
	`eev` string COMMENT '排放标准',
	`boxcar_len` decimal(16,2) COMMENT '货箱长（m）',
	`boxcar_wd` decimal(16,2) COMMENT '货箱宽（m）',
	`boxcar_hg` decimal(16,2) COMMENT '货箱高（m）',
	`max_speed` bigint COMMENT '最高时速（千米/时）',
	`oil_vol` bigint COMMENT '油箱容积（升）',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '卡车型号全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_truck_model_full';

drop table if exists ods_truck_team_full;
create external table ods_truck_team_full(
	`id` bigint COMMENT '车队ID',
	`name` string COMMENT '车队名称',
	`team_no` string COMMENT '车队编号',
	`org_id` bigint COMMENT '所属机构',
	`manager_emp_id` bigint COMMENT '负责人',
	`create_time` string COMMENT '创建时间',
	`update_time` string COMMENT '更新时间',
	`is_deleted` string COMMENT '删除标记（0:不可用 1:可用）'
) comment '车队信息全量表'
partitioned by (`dt` string comment '统计日期')
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/tms/ods/ods_truck_team_full';