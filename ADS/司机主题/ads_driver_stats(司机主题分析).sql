drop table if exists ads_driver_stats;
create external table ads_driver_stats(
	`dt` string COMMENT '统计日期',
	`recent_days` tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
	`driver_emp_id` bigint comment '第一司机员工ID',
	`driver_name` string comment '第一司机姓名',
	`trans_finish_count` bigint COMMENT '完成运输次数',
	`trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
	`trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
	`avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
	`avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒',
	`trans_finish_late_count` bigint COMMENT '逾期次数'
) comment '司机分析'
row format delimited fields terminated by '\t'
location '/warehouse/tms/ads/ads_driver_stats';



-- 数据装载
insert overwrite table ads_driver_stats
select
	dt,
	recent_days,
	driver_emp_id,
	driver_name,
	trans_finish_count,
	trans_finish_distance,
	trans_finish_dur_sec,
	avg_trans_finish_distance,
	avg_trans_finish_dur_sec,
	trans_finish_late_count
from ads_driver_stats
union
select
	'2024-01-08' dt,
	recent_days,
	driver1_emp_id	driver_id,
	driver1_name driver_name,
	sum(trans_finish_count) trans_finish_count,
	sum(trans_finish_distance) trans_finish_distance,
	sum(trans_finish_dur_sec) trans_finish_dur_sec,
	sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
	sum(trans_finish_dur_sec) / sum(trans_finish_count) avg_trans_finish_dur_sec,
	sum(trans_finish_delay_count) trans_finish_late_count
from dws_trans_shift_trans_finish_nd
where dt = '2024-01-08'
group by
	recent_days,
	driver1_emp_id,
	driver1_name;