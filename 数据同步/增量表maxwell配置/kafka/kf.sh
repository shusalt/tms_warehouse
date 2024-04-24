#!/bin/bash

case $1 in
"start"){
  for i in master slave1 slave2
  do
    echo "-------启动 $i kafka-------"
    ssh $i "/usr/sda3/kafka/bin/kafka-server-start.sh -daemon /usr/sda3/kafka/config/server.properties"
  done
};;
"stop"){
  for i in master slave1 slave2
  do
    echo "------关闭 $i kafka------"
    ssh $i "/usr/sda3/kafka/bin/kafka-server-stop.sh stop"
  done
};;
esac

