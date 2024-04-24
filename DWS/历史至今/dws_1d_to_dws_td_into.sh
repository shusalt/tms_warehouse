#!/bin/bash
# 判断参数是否传入
if [ $# -lt 2 ]; then
	echo "必须传入all/表名与数仓上线日期..."
	exit
fi

APP=tms
HIVE_PATH=/usr/hive-2.1/bin



dws_trans_dispatch_td_sql="
insert overwrite table dws_trans_dispatch_td partition (dt='$2')
select
	sum(order_count) order_count,
	sum(order_amount) order_amount
from dws_trans_dispatch_1d;
"




dws_trans_bound_finish_td_sql="
insert overwrite table dws_trans_bound_finish_td partition (dt='$2')
select
	count(order_id) order_count,
	sum(amount) order_amount
from (
	select
		order_id,
		max(amount) amount
	from dwd_trans_bound_finish_detail_inc
	group by order_id
) bound_info;
"




case $1 in
	"all")
		$HIVE_PATH/hive -e "use tms;${dws_trans_dispatch_td_sql}${dws_trans_bound_finish_td_sql}"
		;;
	dws_trans_dispatch_td_sql | dws_trans_bound_finish_td_sql)
		sql="${1}_sql"
		$HIVE_PATH/hive -e "use tms;${!sql}"
		;;
esac