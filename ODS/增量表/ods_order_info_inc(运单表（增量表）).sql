drop table if exists ods_order_info_inc;
create external table ods_order_info_inc(
	`type` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`order_no`:string,`status`:string,`collect_type`:string,`user_id`:bigint,`receiver_complex_id`:bigint,`receiver_province_id`:string,`receiver_city_id`:string,`receiver_district_id`:string,`receiver_address`:string,`receiver_name`:string,`sender_complex_id`:bigint,`sender_province_id`:string,`sender_city_id`:string,`sender_district_id`:string,`sender_name`:string,`payment_type`:string,`cargo_num`:bigint,`amount`:decimal(16,2),`estimate_arrive_time`:string,`distance`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值' 
) comment '运单增量表'
partitioned by (`dt` string)
row format seder 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_order_info_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec'); 
