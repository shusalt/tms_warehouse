drop table if exists ods_order_org_bound_inc;
create external table ods_order_org_bound_inc(
	`op` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`order_id`:bigint,`org_id`:bigint,`status`:string,`inbound_time`:string,`inbound_emp_id`:bigint,`sort_time`:string,`sorter_emp_id`:bigint,`outbound_time`:string,`outbound_emp_id`:bigint,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '运输机构中转表增量'
partitioned by (`dt` string comment '统计日期')
row foramt serder 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_order_org_bound_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');