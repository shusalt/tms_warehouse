#!/bin/bash

APP='tms'
HIVE_PATH=/usr/hive-2.1/bin

if [ -n "$2" ]; then
	do_date=$2
else
	do_date=`date -d '-1 day' +%F`
fi

load_data(){
	sql=""
	for i in $*; do
		# 判断相关hdfs路径是否存在
		hdfs dfs -test -e /origin_data/tms/${i:4}/$do_date
		if [ $? -eq 0 ]; then
			sql=$sql"load data inpath '/origin_data/tms/${i:4}/$do_date' overwrite into table ${APP}.${i} partition(dt='$do_date');"
		else
			echo "不存在路径/origin_data/tms/${i:4}/$do_date"
		fi
	done
	echo "$sql"
	$HIVE_PATH/hive -e "$sql"
}

case $1 in
	ods_order_info_inc | ods_order_cargo_inc | ods_transport_task_inc | ods_order_org_bound_inc | ods_user_info_inc | ods_user_address_inc | ods_base_complex_full | ods_base_dic_full | ods_base_region_info_full | ods_base_organ_full | ods_express_courier_full | ods_express_courier_complex_full | ods_employee_info_full | ods_line_base_shift_full | ods_line_base_info_full | ods_truck_driver_full | ods_truck_info_full | ods_truck_model_full | ods_truck_team_full)
		load_data $1
		;;
	"all")
		load_data ods_order_info_inc ods_order_cargo_inc ods_transport_task_inc ods_order_org_bound_inc ods_user_info_inc ods_user_address_inc ods_base_complex_full ods_base_dic_full ods_base_region_info_full ods_base_organ_full ods_express_courier_full ods_express_courier_complex_full ods_employee_info_full ods_line_base_shift_full ods_line_base_info_full ods_truck_driver_full ods_truck_info_full ods_truck_model_full ods_truck_team_full
		;;
esac