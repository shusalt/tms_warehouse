drop table if exists dws_trade_org_cargo_type_order_nd;
create external table dws_trade_org_cargo_type_order_nd(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`cargo_type` string comment '货物类型',
	`cargo_type_name` string comment '货物类型名称',
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '下单数',
	`order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单n日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_nd'
tblproperties ('orc.compression' = 'snappy');







drop table if exists dws_trans_org_receive_nd;
create external table dws_trans_org_receive_nd(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '揽收次数',
	`order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收n日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_receive_nd'
tblproperties ('orc.compress'='snappy');






drop table if exists dws_trans_dispatch_nd;
create external table dws_trans_dispatch_nd(
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '发单总数',
	`order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单1日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_dispatch_nd'
tblproperties('orc.compress'='snappy');





drop table if exists dws_trans_shift_trans_finish_nd;
create external table dws_trans_shift_trans_finish_nd(
	`shift_id` bigint comment '班次ID',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`line_id` bigint comment '线路ID',
	`line_name` string comment '线路名称',
	`driver1_emp_id` bigint comment '第一司机员工ID',
	`driver1_name` string comment '第一司机姓名',
	`driver2_emp_id` bigint comment '第二司机员工ID',
	`driver2_name` string comment '第二司机姓名',
	`truck_model_type` string comment '卡车类别编码',
	`truck_model_type_name` string comment '卡车类别名称',
	`recent_days` tinyint comment '最近天数',
	`trans_finish_count` bigint comment '转运完成次数',
	`trans_finish_distance` decimal(16,2) comment '转运完成里程',
	`trans_finish_dur_sec` bigint comment '转运完成时长，单位：秒',
	`trans_finish_order_count` bigint comment '转运完成运单数',
	`trans_finish_delay_count` bigint comment '逾期次数'
) comment '物流域班次粒度转运完成最近n日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_shift_trans_finish_nd/'
tblproperties('orc.compress'='snappy');






drop table if exists dws_trans_org_deliver_suc_nd;
create external table dws_trans_org_deliver_suc_nd(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功n日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_deliver_suc_nd'
tblproperties('orc.compress'='snappy');






drop table if exists dws_trans_org_sort_nd;
create external table dws_trans_org_sort_nd(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`recent_days` tinyint comment '最近天数',
	`sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣n日汇总表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_sort_nd/'
tblproperties('orc.compress'='snappy');