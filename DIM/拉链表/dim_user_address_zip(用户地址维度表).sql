drop table if exists dim_user_address_zip;
create external table dim_user_address_zip(
	`id` bigint COMMENT '地址ID',
	`user_id` bigint COMMENT '用户ID',
	`phone` string COMMENT '电话号',
	`province_id` bigint COMMENT '所属省份ID',
	`city_id` bigint COMMENT '所属城市ID',
	`district_id` bigint COMMENT '所属区县ID',
	`complex_id` bigint COMMENT '所属小区ID',
	`address` string COMMENT '详细地址',
	`is_default` tinyint COMMENT '是否默认',
	`start_date` string COMMENT '起始日期',
	`end_date` string COMMENT '结束日期'
) comment '用户地址维度表'
partitioned by (`dt` string comment)
stored as orc
location '/warehouse/tms/dim/dim_user_address_zip'
tblproperties ('orc.compression' = 'sanppy');





-- 数据装载
-- 首日装载
insert overwrite table dim_user_address_zip partition (dt='9999-12-31')
select
	data.id,
	data.user_id,
	data.phone,
	data.province_id,
	data.city_id,
	data.district_id,
	data.complex_id,
	data.address,
	data.is_default,
	concat(substring(data.create_time, 1, 10), ' ', substring(data.create_time, 12, 8)) start_date,
	'9999-12-31' end_date
from ods_user_address_inc
where type = 'bootstrap-insert' and dt='2024-01-07' and data.is_deleted = '0';




-- 每日装载
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