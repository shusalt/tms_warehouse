drop table if exists ods_order_cargo_inc;
create external table ods_order_cargo_inc(
	`op` string comment '变动类型',
	`ts` string comment '变动时间戳',
	`data` struct<`id`:bigint,`order_id`:string,`cargo_type`:string,`volume_length`:bigint,`volume_width`:bigint,`volume_height`:bigint,`weight`:decimal(16,2),`create_time`:string,`update_time`:string,`is_deleted`:string> comment '数据',
	`old` map<string, string> comment '旧值'
) comment '运单明细增量表'
partitioned by (`dt` string comment '统计日期');
row format serder 'org.apache.hive.hcatalog.data.JsonSerDe'
location '/warehouse/tms/ods/ods_order_cargo_inc'
tblproperties ('compression.codec' = 'org.apache.hadoop.io.compress.GzipCodec');