#!/bin/bash

APP=tms
HIVE_PATH=/usr/hive-2.1/bin

if [ $# -le 1 ]; then
	echo '必须传入all/表名与数仓上线时间'
	exit
fi


dim_complex_full_sql="
insert overwrite table tms.dim_complex_full partition (dt = '$2')
select
	complex_info.id id,
	complex_name,
	courier_emp_ids,
	province_id,
	dic_for_prov.name province_name,
	city_id,
	dic_for_city.name city_name,
	district_id,
	district_name
from (
	select
		id,
		complex_name,
		province_id,
		city_id,
		district_id,
		district_name
    from ods_base_complex_full
    where dt = '$2' and is_deleted = '0'
) complex_info
join(
	select 
		id,
		name
	from ods_base_region_info_full
	where dt = '$2' and is_deleted = '0'
) dic_for_prov
on complex_info.province_id = dic_for_prov.id
join (
	select
		id,
		name
	from ods_base_region_info_full
	where dt = '$2' and is_deleted = '0'
) dic_for_city
on complex_info.city_id = dic_for_city.id
left join(
	select 
		collect_set(cast(courier_emp_id as string)) courier_emp_ids,
		complex_id
	from ods_express_courier_complex_full
	where dt = '$2' and is_deleted = '0'
group by complex_id
) complex_courier
on complex_info.id = complex_courier.complex_id;
"


# 机构维度表
dim_organ_full_sql="
insert overwrite table tms.dim_organ_full partition (dt = '$2')
select
	organ_info.id,
	organ_info.org_name,
	org_level,
	region_id,
	region_info.name        region_name,
	region_info.dict_code   region_code,
	org_parent_id,
	org_for_parent.org_name org_parent_name
from (
	select id,
        org_name,
        org_level,
        region_id,
        org_parent_id
	from ods_base_organ_full
	where dt = '$2' and is_deleted = '0') organ_info
left join (
    select
    	id,
		name,
		dict_code
    from ods_base_region_info_full
	where dt = '$2' and is_deleted = '0'
) region_info
on organ_info.region_id = region_info.id
left join (
    select 
		id,
		org_name
    from ods_base_organ_full
    where dt = '$2' and is_deleted = '0'
) org_for_parent
on organ_info.org_parent_id = org_for_parent.id;
"


# 地区维度表
dim_region_full_sql="
insert overwrite table tms.dim_region_full partition (dt ='$2')
select
	id,
	parent_id,
	name,
	dict_code,
	short_name
from ods_base_region_info_full
where dt = '$2' and is_deleted = '0';
"


# 快递员维度表
dim_express_courier_full_sql="
insert overwrite table tms.dim_express_courier_full partition (dt='$2')
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
	where dt = '$2' and is_deleted = '0'
) cour
inner join (
	select
		id,
		org_name
	from ods_base_organ_full
	where dt = '$2' and is_deleted = '0'
) org
on cour.org_id = org.id
inner join (
	select
		id,
		name express_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) dic
on cour.express_type = dic.id;
"


# 班次维度表
dim_shift_full_sql="
insert overwrite table tms.dim_shift_full partition (dt='$2')
SELECT 
	shift.id,
	line_id,
	line_name,
	line_no,
	line_level,
	org_id,
	transport_line_type_id,
	dic.transport_line_type_name,
	start_org_id,
	start_org_name,
	end_org_id,
	end_org_name,
	pair_line_id,
	distance,
	cost,
	estimated_time,
	start_time,
	driver1_emp_id,
	driver2_emp_id,
	truck_id,
	pair_shift_id
from (
	select
		id,
		line_id,
		start_time,
		driver1_emp_id,
		driver2_emp_id,
		truck_id,
		pair_shift_id
	from ods_line_base_shift_full
	where dt = '$2' and is_deleted = '0'
) shift
inner join (
	select
		id,
		name line_name,
		line_no,
		line_level,
		org_id,
		transport_line_type_id,
		start_org_id,
		start_org_name,
		end_org_id,
		end_org_name,
		pair_line_id,
		distance,
		cost,
		estimated_time
	from ods_line_base_info_full olbif
	where dt = '$2' and is_deleted = '0'
) line_info
on shift.line_id = line_info.id
inner join (
	select
		id,
		name transport_line_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) dic
on line_info.transport_line_type_id = dic.id;
"


# 司机维度表
dim_truck_driver_full_sql="
insert overwrite table tms.dim_truck_driver_full partition (dt='$2')
select
	driver.id,
	emp_id,
	org_id,
	org_name,
	team_id,
	tream_name,
	license_type,
	init_license_date,
	expire_date,
	license_no,
	is_enabled
from(
	select
		id,
		emp_id,
		org_id,
		team_id,
		license_type,
		init_license_date,
		expire_date,
		license_no,
		is_enabled
	from ods_truck_driver_full
	where dt = '$2' and is_deleted = '0'
) driver
inner join (
	select
		id,
		org_name
	from ods_base_organ_full
	where dt = '$2' and is_deleted = '0'
) organ
on driver.org_id = organ.id
inner join (
	select
		id,
		name tream_name
	from ods_truck_team_full
	where dt = '$2' and is_deleted = '0'
) team
on driver.team_id = team.id;
"


# 卡车维度表
dim_truck_full_sql="
insert overwrite table tms.dim_truck_full partition (dt='$2')
select
	truck_info.id,
	team_id,
	team_name,
	team_no,
	org_id,
	org_name,
	manager_emp_id,
	truck_no,
	truck_model_id,
	truck_model_name,
	truck_model_type,
	truck_model_type_name,
	truck_model_no,
	truck_brand,
	truck_brand_name,
	truck_weight,
	load_weight,
	total_weight,
	eev,
	boxcar_len,
	boxcar_wd,
	boxcar_hg,
	max_speed,
	oil_vol,
	device_gps_id,
	engine_no,
	license_registration_date,
	license_last_check_date,
	license_expire_date,
	is_enabled
from (
	select
		id,
		team_id,
		truck_no,
		truck_model_id,
		device_gps_id,
		engine_no,
		license_registration_date,
		license_last_check_date,
		license_expire_date,
		is_enabled
	from ods_truck_info_full
	where dt = '$2' and is_deleted = '0'
) truck_info
inner join (
	select
		id,
		model_name truck_model_name,
		model_type truck_model_type,
		model_no truck_model_no,
		brand truck_brand,
		truck_weight,
		load_weight,
		total_weight,
		eev,
		boxcar_len,
		boxcar_wd,
		boxcar_hg,
		max_speed,
		oil_vol
	from ods_truck_model_full
	where dt = '$2' and is_deleted = '0'
) truck_model
on truck_info.truck_model_id = truck_model.id
inner join (
	select
		id,
		name truck_model_type_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) dic_type
on truck_model.truck_model_type = dic_type.id
inner join (
	select
		id,
		name truck_brand_name
	from ods_base_dic_full
	where dt = '$2' and is_deleted = '0'
) dic_brand
on truck_model.truck_brand = dic_brand.id
inner join (
	select
		id,
		name team_name,
		team_no,
		org_id,
		manager_emp_id
	from ods_truck_team_full
	where dt = '$2' and is_deleted = '0'
) team
on truck_info.team_id = team.id
inner join (
	select
		id,
		org_name
	from ods_base_organ_full
	where dt = '$2' and is_deleted = '0'
) organ
on team.org_id = organ.id;
"


dim_user_zip_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dim_user_zip partition (dt)
select
	id,
	login_name,
	nick_name,
	passwd,
	real_name,
	phone_num,
	email,
	user_level,
	birthday,
	gender,
	start_date,
	if(rk=1, end_date, date_add('2024-01-08', -1)) end_date,
	if(rk=1, end_date, date_add('2024-01-08', -1)) dt -- 动态分区
from (
	select
		id,
		login_name,
		nick_name,
		passwd,
		real_name,
		phone_num,
		email,
		user_level,
		birthday,
		gender,
		start_date,
		end_date,
		row_number() over(partition by id order by start_date desc) rk
	from (
	-- 合并拉链表与变化表形成临时拉链表
		-- 拉链表
		select
			id,
			login_name,
			nick_name,
			passwd,
			real_name,
			phone_num,
			email,
			user_level,
			birthday,
			gender,
			start_date,
			end_date
		from dim_user_zip
		where dt = '9999-12-31'
		union
		-- 变化的数据
		select
			id,
			login_name,
			nick_name,
			passwd,
			real_name,
			phone_num,
			email,
			user_level,
			birthday,
			gender,
			start_date,
			end_date
		from (
			select
				data.id,
				data.login_name,
				data.nick_name,
				data.passwd,
				data.real_name,
				data.phone_num,
				data.email,
				data.user_level,
				data.birthday,
				data.gender,
				date_format(from_unixtime(cast(ts as bigint)), 'yyyy-MM-dd') start_date,
				'9999-12-31' end_date,
				-- 因为数据源模拟程序,没有很好模拟数据发送的变量,发生update操作时获取的时间戳,与最初insert操作时获取的时间戳是在同一个时间的
				-- 本应该只需求按照ts倒排的,当时这里为获取到最新的数据,这能在按照type排序,update操作排到最前面
				row_number() over(partition by data.id order by ts desc, type desc) rk
			from ods_user_info_inc
			where dt = '2024-01-08' and data.is_deleted = '0'	
		) aa
		where rk = 1
	) bb
) cc;
"

dim_user_address_zip_sql="
set hive.exec.dynamic.partition.mode=nostrict;
insert overwrite table dim_user_address_zip partition (dt)
-- 覆盖原理的拉链表，实现更新操作
select
	id,
	user_id,
	phone,
	province_id,
	city_id,
	district_id,
	complex_id,
	address,
	is_default,
	start_date,
	if(rk2=1, end_date, date_add('2024-01-08', -1)) end_date,
	if(rk2=1, end_date, date_add('2024-01-08', -1)) dt -- 动态分区
from (
	select
		id,
		user_id,
		phone,
		province_id,
		city_id,
		district_id,
		complex_id,
		address,
		is_default,
		start_date,
		end_date,
		row_number() over(partition by id order by start_date desc) rk2
	from (
		-- 拉链表数据
		select
			id,
			user_id,
			phone,
			province_id,
			city_id,
			district_id,
			complex_id,
			address,
			is_default,
			start_date,
			end_date
		from dim_user_address_zip
		where dt = '9999-12-31'
		union
		-- 变化的数据
		select
			id,
			user_id,
			phone,
			province_id,
			city_id,
			district_id,
			complex_id,
			address,
			is_default,
			start_date,
			end_date
		from (
			select
				data.id,
				data.user_id,
				data.phone,
				data.province_id,
				data.city_id,
				data.district_id,
				data.complex_id,
				data.address,
				cast(data.is_default as tinyint) is_default,
				concat(substring(from_unixtime(cast(ts as bigint)), 1, 10), ' ', substring(from_unixtime(cast(ts as bigint)), 12, 8)) start_date,
				'9999-12-31' end_date,
				-- 挑选最新的数据
				row_number() over(partition by data.id order by ts desc) rk
			from ods_user_address_inc
			where type in ('insert','update') and dt = '2024-01-08' and data.is_deleted = '0'
		) aa
		where rk = 1
	) bb
) cc;
"


exec_func(){
	for i in $*; do
		echo "${!i}"
		$HIVE_PATH/hive -e "use tms;${!i}"
	done
}


case $1 in
	"all")
		exec_func dim_complex_full_sql dim_organ_full_sql dim_region_full_sql dim_express_courier_full_sql dim_shift_full_sql dim_truck_driver_full_sql dim_truck_full_sql dim_user_zip_sql dim_user_address_zip_sql
		;;
	dim_complex_full | dim_organ_full | dim_region_full | dim_express_courier_full | dim_shift_full | dim_truck_driver_full | dim_truck_full | dim_user_zip | dim_user_address_zip)
		sql="${1}_sql"
		exec_func $sql
		;;
	*)
		echo "tablename error....."
		;;
esac