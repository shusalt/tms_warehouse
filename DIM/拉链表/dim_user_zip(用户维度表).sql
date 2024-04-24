drop table if exists dim_user_zip;
create external table dim_user_zip(
	`id` bigint COMMENT '用户地址信息ID',
	`login_name` string COMMENT '用户名称',
	`nick_name` string COMMENT '用户昵称',
	`passwd` string COMMENT '用户密码',
	`real_name` string COMMENT '用户姓名',
	`phone_num` string COMMENT '手机号',
	`email` string COMMENT '邮箱',
	`user_level` string COMMENT '用户级别',
	`birthday` string COMMENT '用户生日',
	`gender` string COMMENT '性别 M男,F女',
	`start_date` string COMMENT '起始日期',
	`end_date` string COMMENT '结束日期'
) comment '用户维度表'
partitioned by (`dt` string comment '统计日期')
stored as orc
location '/warehouse/tms/dim/dim_user_zip'
tblproperties ('orc.compression' = 'sanppy');





-- 数据装载
-- 首日装载
insert overwrite table dim_user_zip partition (dt='2024-01-07')
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
	date_format(data.create_time, 'yyyy-MM-dd') start_date,
	'9999-12-31' end_date
from ods_user_info_inc
where dt = '2024-01-07' and type = 'bootstrap-insert' and data.is_deleted = '0';



-- 每日装载
-- 拉链表
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