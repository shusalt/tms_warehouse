#!/bin/bash

# 该脚本的作用是初始化所有的增量表，只需执行一次
MAXWELL_HOME=/usr/sda3/maxwell-1.29.2

import_data() {
 $MAXWELL_HOME/bin/maxwell-bootstrap --database tms --table $1 --config $MAXWELL_HOME/config_file/tms_config.properties
}

case $1 in
"order_info" | "order_cargo" | "transport_task" | "ordre_org_bound" | "user_info" | "user_address")
  import_data $1
  ;;
"all")
  for table in "order_info"  "order_cargo"  "transport_task"  "ordre_org_bound"  "user_info" "user_address";
  do
    #statements
    import_data $table
  done
  ;;
esac