drop table if exists ods_transport_task_inc;
create external table ods_transport_task_inc(
	`op` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`shift_id`:bigint,`line_id`:bigint,`start_org_id`:bigint,`start_org_name`:string,`end_org_id`:bigint,`end_org_name`:string,`status`:string,`order_num`:bigint,`driver1_emp_id`:bigint,`driver1_name`:string,`driver2_emp_id`:bigint,`driver2_name`:string,`truck_id`:bigint,`truck_no`:string,`actual_start_time`:string,`actual_end_time`:string,`actual_distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '插入或者修改后的数据',
	`old` map<string, string>
) comment '运输任务增量表'
partitioned by (`dt` string comment '统计日期')
row format serder 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_transport_task_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');