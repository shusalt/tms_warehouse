drop table if exists ods_user_address_inc;
create external table ods_user_address_inc(
	`op` string comment '变动操作',
	`ts` string comment '变更时间戳',
	`data` struct<`id`:bigint,`user_id`:bigint,`phone`:string,`province_id`:bigint,`city_id`:bigint,`district_id`:bigint,`complex_id`:bigint,`address`:string,`is_default`:string,`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '用户地址增量表'
partitioned by (`dt` string comment '统计日期')
row format serder 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_user_address_inc';