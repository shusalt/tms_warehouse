#!/bin/bash

# maxwell启停脚本
MAXWELL_HOME=/usr/sda3/maxwell-1.29.2
MAXWELL_CONF=$2


status_maxwell() {
	result=`ps -ef | grep com.zendesk.maxwell.Maxwell | grep -v grep | wc -l`
	return $result
}


start_maxwell() {
	status_maxwell
	if [[ $? -lt 1 ]] ;then
		echo "启动Maxwell"
		$MAXWELL_HOME/bin/maxwell --config $MAXWELL_HOME/config_file/tms_config.porperties --daemon
	else
		echo "Maxwell正在运行"
	fi
}


stop_maxwell() {
	status_maxwell
	if [[ $? -gt 0 ]]; then
		echo "停止Maxwell"
		ps -ef | grep com.zendesk.maxwell.Maxwell | grep -v grep | awk '{print $2}' | xargs kill -9
	else
		echo "Maxwell未在运行"
	fi
}



case $1 in
	start )
        start_maxwell
    ;;
    stop )
        stop_maxwell
    ;;
    restart )
        stop_maxwell
        start_maxwell
    ;;
esac