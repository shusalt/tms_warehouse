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



-- 数据装载
insert overwrite table dws_trans_shift_trans_finish_nd partition (dt='2024-01-08')
select
	shift_id,
	city_id,
	city_name,
	start_org_id org_id,
	start_org_name org_name,
	line_id,
	line_name,
	driver1_emp_id,
	driver1_name,
	driver2_emp_id,
	driver2_name,
	truck_model_type,
	truck_model_type_name,
	recent_days,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	trans_finish_order_count,
	trans_finish_delay_count
from (
	-- 先聚合在关联维度名称，事实表里已有维度id
	select
		shift_id,
		line_id,
		start_org_id,
		start_org_name,
		driver1_emp_id,
		driver1_name,
		driver2_emp_id,
		driver2_name,
		truck_id,
		recent_days,
		count(id) trans_finish_count,
		sum(actual_distance) trans_finish_distance,
		sum(finish_dur_sec) trans_finish_dur_sec,
		sum(order_num) trans_finish_order_count,
		sum(if(actual_end_time > estimate_end_time, 1, 0)) trans_finish_delay_count
	from dwd_trans_trans_finish_inc
	lateral view explode(array(7, 30)) tmp as recent_days
	where dt >= date_add('2024-01-08', -recent_days+1)
	group by
		shift_id,
		line_id,
		start_org_id,
		start_org_name,
		driver1_emp_id,
		driver1_name,
		driver2_emp_id,
		driver2_name,
		truck_id,
		recent_days
) trans_info
left join (
	select
		organ_info.id,
		if(org_level=1, dim_city.id, dim_province.id) city_id,
		if(org_level=1, dim_city.name, dim_province.name) city_name
	from (
		select
			id,
			org_level,
			region_id
		from dim_organ_full
		where dt = '2024-01-07'
	) organ_info
	left join (
		select
			id,
			name,
			parent_id
		from dim_region_full
		where dt = '2024-01-07'
	) dim_city
	on organ_info.region_id = dim_city.id
	left join (
		select
			id,
			name
		from dim_region_full
		where dt = '2024-01-07'
	) dim_province
	on dim_city.parent_id = dim_province.id
) city_info
on trans_info.start_org_id = city_info.id
left join (
	select
		id,
		line_name
	from dim_shift_full
	where dt = '2024-01-07'
) shift_info
on trans_info.shift_id = shift_info.id
left join (
	select
		id,
		truck_model_type,
		truck_model_type_name
	from dim_truck_full
	where dt = '2024-01-07'
) truck_info
on trans_info.truck_id = truck_info.id;