-- 交易域取消运单事务事实表
drop table if exists dwd_trade_order_cancel_detail_inc;
create external table dwd_trade_order_cancel_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`cancel_time` string COMMENT '取消时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '交易域取消运单事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_order_cancel_detail_inc'
tblproperties ('orc.compression' = 'snappy');



-- 交易域订单明细事务事实表
drop table if exists dwd_trade_order_detail_inc;
create external table dwd_trade_order_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`order_time` string COMMENT '下单时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '交易域订单明细事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_order_detail_inc'
tblproperties ('orc.compression' = 'snappy');




-- 交易域运单累积快照事实表
drop table if exists dwd_trade_order_process_inc;
create external table dwd_trade_order_process_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`order_time` string COMMENT '下单时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳',
	`start_date` string COMMENT '开始日期',
	`end_date` string COMMENT '结束日期'
) comment '交易域运单累积快照事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_order_process_inc'
tblproperties ('orc.compression' = 'snappy');




-- 交易域支付成功事务事实表
drop table if exists dwd_trade_pay_suc_detail_inc;
create external table dwd_trade_pay_suc_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`payment_time` string COMMENT '支付时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '交易域支付成功事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_pay_suc_detail_inc'
tblproperties ('orc.compression' = 'snappy');





-- 物流域转运完成事务事实表
drop table if exists dwd_trans_bound_finish_detail_inc;
create external table dwd_trans_bound_finish_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`bound_finish_time` string COMMENT '转运完成时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '物流域转运完成事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_bound_finish_detail_inc'
tblproperties ('orc.compression' = 'snappy');





-- 物流域派送完成事务事实表
drop table if exists dwd_trans_deliver_suc_detail_inc;
create external table dwd_trans_deliver_suc_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`deliver_suc_time` string COMMENT '派送成功时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '物流域派送完成事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_deliver_suc_detail_inc'
tblproperties ('orc.compression' = 'snappy');





-- 物流域发单事务事实表
drop table if exists dwd_trans_dispatch_detail_inc;
create external table dwd_trans_dispatch_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`dispatch_time` string COMMENT '发单时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '物流域发单事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_dispatch_detail_inc'
tblproperties ('orc.compression' = 'snappy');





-- 物流域揽收事务事实表
drop table if exists dwd_trans_receive_detail_inc;
create external table dwd_trans_receive_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`receive_time` string COMMENT '揽收时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '物流域揽收事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd_trans_receive_detail_inc'
tblproperties ('orc.compression' = 'snappy');






-- 物流域签收事务事实表
drop table if exists dwd_trans_sign_detail_inc;
create external table dwd_trans_sign_detail_inc(
	`id` bigint comment '运单明细ID',
	`order_id` string COMMENT '运单ID',
	`cargo_type` string COMMENT '货物类型ID',
	`cargo_type_name` string COMMENT '货物类型名称',
	`volumn_length` bigint COMMENT '长cm',
	`volumn_width` bigint COMMENT '宽cm',
	`volumn_height` bigint COMMENT '高cm',
	`weight` decimal(16,2) COMMENT '重量 kg',
	`sign_time` string COMMENT '签收时间',
	`order_no` string COMMENT '运单号',
	`status` string COMMENT '运单状态',
	`status_name` string COMMENT '运单状态名称',
	`collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
	`collect_type_name` string COMMENT '取件类型名称',
	`user_id` bigint COMMENT '用户ID',
	`receiver_complex_id` bigint COMMENT '收件人小区id',
	`receiver_province_id` string COMMENT '收件人省份id',
	`receiver_city_id` string COMMENT '收件人城市id',
	`receiver_district_id` string COMMENT '收件人区县id',
	`receiver_name` string COMMENT '收件人姓名',
	`sender_complex_id` bigint COMMENT '发件人小区id',
	`sender_province_id` string COMMENT '发件人省份id',
	`sender_city_id` string COMMENT '发件人城市id',
	`sender_district_id` string COMMENT '发件人区县id',
	`sender_name` string COMMENT '发件人姓名',
	`payment_type` string COMMENT '支付方式',
	`payment_type_name` string COMMENT '支付方式名称',
	`cargo_num` bigint COMMENT '货物个数',
	`amount` decimal(16,2) COMMENT '金额',
	`estimate_arrive_time` string COMMENT '预计到达时间',
	`distance` decimal(16,2) COMMENT '距离，单位：公里',
	`ts` bigint COMMENT '时间戳'
) comment '物流域签收事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_sign_detail_inc'
tblproperties ('orc.compression' = 'snappy');







-- 物流域运输完成事务事实表
drop table if exists dwd_trans_trans_finish_inc;
create external table dwd_trans_trans_finish_inc(
	`id` bigint comment '运输任务ID',
	`shift_id` bigint COMMENT '车次ID',
	`line_id` bigint COMMENT '路线ID',
	`start_org_id` bigint COMMENT '起始机构ID',
	`start_org_name` string COMMENT '起始机构名称',
	`end_org_id` bigint COMMENT '目的机构ID',
	`end_org_name` string COMMENT '目的机构名称',
	`order_num` bigint COMMENT '运单个数',
	`driver1_emp_id` bigint COMMENT '司机1ID',
	`driver1_name` string COMMENT '司机1名称',
	`driver2_emp_id` bigint COMMENT '司机2ID',
	`driver2_name` string COMMENT '司机2名称',
	`truck_id` bigint COMMENT '卡车ID',
	`truck_no` string COMMENT '卡车号牌',
	`actual_start_time` string COMMENT '实际启动时间',
	`actual_end_time` string COMMENT '实际到达时间',
	`estimate_end_time` string COMMENT '预估到达时间',
	`actual_distance` decimal(16,2) COMMENT '实际行驶距离',
	`finish_dur_sec` bigint COMMENT '运输完成历经时长：秒',
	`ts` bigint COMMENT '时间戳'
) comment '物流域运输完成事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_trans_finish_inc'
tblproperties ('orc.compression' = 'snappy');








-- 中转域入库事务事实表
drop table if exists dwd_bound_inbound_inc;
create external table dwd_bound_inbound_inc(
	`id` bigint COMMENT '中转记录ID',
	`order_id` bigint COMMENT '运单ID',
	`org_id` bigint COMMENT '机构ID',
	`inbound_time` string COMMENT '入库时间',
	`inbound_emp_id` bigint COMMENT '入库人员'
) comment '中转域入库事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_inbound_inc'
tblproperties ('orc.compression' = 'snappy');





-- 中转域出库事务事实表
drop table if exists dwd_bound_outbound_inc;
create external table dwd_bound_outbound_inc(
	`id` bigint COMMENT '中转记录ID',
	`order_id` bigint COMMENT '订单ID',
	`org_id` bigint COMMENT '机构ID',
	`outbound_time` string COMMENT '出库时间',
	`outbound_emp_id` bigint COMMENT '出库人员'
) comment '中转域出库事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_outbound_inc'
tblproperties ('orc.compression' = 'snappy');




-- 中转域分拣事务事实表
drop table if exists dwd_bound_sort_inc;
create external table dwd_bound_sort_inc(
	`id` bigint COMMENT '中转记录ID',
	`order_id` bigint COMMENT '订单ID',
	`org_id` bigint COMMENT '机构ID',
	`sort_time` string COMMENT '分拣时间',
	`sorter_emp_id` bigint COMMENT '分拣人员'
) comment '中转域分拣事务事实表'
partitioned by (dt string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_sort_inc'
tblproperties ('orc.compression' = 'snappy');