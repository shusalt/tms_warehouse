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




-- 数据装载
-- 首日装载
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dwd_trans_trans_finish_inc partition(dt)
select
	info.id,
	info.shift_id,
	info.line_id,
	info.start_org_id,
	info.start_org_name,
	info.end_org_id,
	info.end_org_name,
	info.order_num,
	info.driver1_emp_id,
	info.driver1_name,
	info.driver2_emp_id,
	info.driver2_name,
	info.truck_id,
	info.truck_no,
	info.actual_start_time,
	info.actual_end_time,
	date_format(from_unixtime(unix_timestamp(info.actual_start_time, 'yyyy-MM-dd HH:mm:ss') + shift_info.estimated_time * 60, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') estimate_end_time,
	info.actual_distance,
	cast(info.finish_dur_sec as bigint) finish_dur_sec,
	info.ts,
	info.dt
from (
	select
		data.id,
		data.shift_id,
		data.line_id,
		data.start_org_id,
		data.start_org_name,
		data.end_org_id,
		data.end_org_name,
		data.order_num,
		data.driver1_emp_id,
		data.driver1_name,
		data.driver2_emp_id,
		data.driver2_name,
		data.truck_id,
		data.truck_no,
		data.actual_start_time,
		data.actual_end_time,
		data.actual_distance,
		unix_timestamp(data.actual_end_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(data.actual_start_time, 'yyyy-MM-dd HH:mm:ss') finish_dur_sec,
		ts,
		date_format(data.actual_end_time, 'yyyy-MM-dd') dt
	from ods_transport_task_inc
	where dt = '2024-01-07' and data.is_deleted = '0'
		and data.actual_end_time is not null 
		and type = 'bootstrap-insert'
) info
left join (
	select
		id,
		estimated_time
	from dim_shift_full
	where dt = '2024-01-07'
)  shift_info
on info.shift_id = shift_info.id;



-- 每日装载
insert overwrite table dwd_trans_trans_finish_inc partition (dt='2024-01-08')
select
	info.id,
	info.shift_id,
	info.line_id,
	info.start_org_id,
	info.start_org_name,
	info.end_org_id,
	info.end_org_name,
	info.order_num,
	info.driver1_emp_id,
	info.driver1_name,
	info.driver2_emp_id,
	info.driver2_name,
	info.truck_id,
	info.truck_no,
	info.actual_start_time,
	info.actual_end_time,
	date_format(from_unixtime(unix_timestamp(info.actual_start_time, 'yyyy-MM-dd HH:mm:ss') + shift_info.estimated_time * 60, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') estimate_end_time,
	info.actual_distance,
	cast(info.finish_dur_sec as bigint) finish_dur_sec,
	info.ts
from (
	select
		data.id,
		data.shift_id,
		data.line_id,
		data.start_org_id,
		data.start_org_name,
		data.end_org_id,
		data.end_org_name,
		data.order_num,
		data.driver1_emp_id,
		data.driver1_name,
		data.driver2_emp_id,
		data.driver2_name,
		data.truck_id,
		data.truck_no,
		data.actual_start_time,
		data.actual_end_time,
		data.actual_distance,
		unix_timestamp(data.actual_end_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(data.actual_start_time, 'yyyy-MM-dd HH:mm:ss') finish_dur_sec,
		ts,
		date_format(data.actual_end_time, 'yyyy-MM-dd') dt
	from ods_transport_task_inc
	where dt = '2024-01-08'
		-- 提取变化的数
		and type = 'update'
		-- actual_end_time不为空，表示两个机构之间的一次运输已完成
		and data.actual_end_time is not null
		and old["actual_end_time"] is null
) info
left join (
	select
		id,
		estimated_time
	from dim_shift_full
	where dt = '2024-01-07'
)  shift_info
on info.shift_id = shift_info.id;