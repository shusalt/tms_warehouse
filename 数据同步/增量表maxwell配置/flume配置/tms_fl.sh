#!/bin/bash

case $1 in
"start")
        echo " --------启动 tms 业务数据flume-------"
        nohup /usr/sda3/apache-flume-1.9.0-bin/bin/flume-ng agent -n a1 -c /usr/sda3/apache-flume-1.9.0-bin/conf -f /usr/sda3/apache-flume-1.9.0-bin/job/tms_kafka_to_hdfs.conf >/dev/null 2>&1 &
;;
"stop")
        echo " --------停止 tms 业务数据flume-------"
        ps -ef | grep tms_kafka_to_hdfs | grep -v grep | awk '{print $2}' | xargs -n1 kill
;;
esac