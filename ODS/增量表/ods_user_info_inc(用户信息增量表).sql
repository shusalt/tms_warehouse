drop table if exists ods_user_info_inc;
create external table ods_user_info_inc(
	`op` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`login_name`:string,`nick_name`:string,`passwd`:string,`real_name`:string,`phone_num`:string,`email`:string,`user_level`:string,`birthday`:string,`gender`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '用户信息增量表'
partitioned by (`dt` string comment '统计日期')
row foramt serder 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_user_info_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');