drop table if exists dim_express_courier_full;
create external exists dim_express_courier_full(
	`id` bigint COMMENT '快递员ID',
	`emp_id` bigint COMMENT '员工ID',
	`org_id` bigint COMMENT '所属机构ID',
	`org_name` string COMMENT '机构名称',
	`working_phone` string COMMENT '工作电话',
	`express_type` string COMMENT '快递员类型（收货；发货）',
	`express_type_name` string COMMENT '快递员类型名称'
) comment '快递员维度表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dim/dim_express_courier_full'
tblproperties ('orc.compression' = 'snappy')



-- 数据转载
insert overwrite table dim_express_courier_full partition (dt='2024-01-07')
select
	cour.id,
	emp_id,
	org_id,
	org.org_name,
	working_phone,
	express_type,
	dic.express_type_name
from (
	select
		id,
		emp_id,
		org_id,
		working_phone,
		express_type	
	from ods_express_courier_full
	where dt = '2024-01-07' and is_deleted = '0'
) cour
inner join (
	select
		id,
		org_name
	from ods_base_organ_full
	where dt = '2024-01-07' and is_deleted = '0'
) org
on cour.org_id = org.id
inner join (
	select
		id,
		name express_type_name
	from ods_base_dic_full
	where dt = '2024-01-07' and is_deleted = '0'
) dic
on cour.express_type = dic.id