#!/bin/bash

APP=tms
HIVE_PATH=/usr/hive-2.1/bin



if [ $# -lt 1 ]; then
	echo "必须传入all/表名..."
	exit
fi


[ $2 ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)


dws_trans_bound_finish_td_sql="
insert overwrite table dws_trans_bound_finish_td partition (dt='$datestr')
select
	sum(order_count) order_count,
	sum(order_amount) order_amount
from (
	select
		order_count,
		order_amount
	from dws_trans_bound_finish_td
	where dt >= date_add('$datestr', -1)
	union
	select
		count(order_id) order_count,
		sum(amount) order_amount
	from (
		select
			order_id,
			max(amount) amount
		from dwd_trans_bound_finish_detail_inc
		where dt = '$datestr'
		group by order_id
	) bound_info
) final_tb;
"



case $1 in
	"all")
		$HIVE_PATH/hive -e "use tms;${dws_trans_bound_finish_td_sql}"
		;;

	dws_trans_bound_finish_td)
		sql="${1}_sql"
		$HIVE_PATH/hive -e "use tms;${!sql}"
		;;
esac