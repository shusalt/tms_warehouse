drop table if exists dws_trade_org_cargo_type_order_1d;
create external table dws_trade_org_cargo_type_order_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`cargo_type` string comment '货物类型',
	`cargo_type_name` string comment '货物类型名称',
	`order_count` bigint comment '下单数',
	`order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_1d'
tblproperties ('orc.compression' = 'snappy');




drop table if exists dws_trans_org_receive_1d;
create external table dws_trans_org_receive_1d(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`order_count` bigint comment '揽收次数',
	`order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_receive_1d'
tblproperties ('orc.compression' = 'snappy');





drop table if exists dws_trans_dispatch_1d;
create external table dws_trans_dispatch_1d(
	`order_count` bigint comment '发单总数',
	`order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_dispatch_1d'
tblproperties ('orc.compression' = 'snappy');





drop table if exists dws_trans_org_truck_model_type_trans_finish_1d;
create external table dws_trans_org_truck_model_type_trans_finish_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`truck_model_type` string comment '卡车类别编码',
	`truck_model_type_name` string comment '卡车类别名称',
	`trans_finish_count` bigint comment '运输完成次数',
	`trans_finish_distance` decimal(16,2) comment '运输完成里程',
	`trans_finish_dur_sec` bigint comment '运输完成时长，单位：秒'
) comment '物流域机构卡车类别粒度运输最近1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_truck_model_type_trans_finish_1d'
tblproperties ('orc.compression' = 'snappy');






drop table if exists dws_trans_org_sort_1d;
create external table dws_trans_org_sort_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_sort_1d'
tblproperties ('orc.compression' = 'snappy');







drop table if exists dws_trans_org_deliver_suc_1d;
create external table dws_trans_org_deliver_suc_1d(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_deliver_suc_1d'
tblproperties ('orc.compression' = 'snappy');