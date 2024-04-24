echo ==========zk节点状态==========

for i in master slave1 slave2

do
       echo ==========$i==========
       ssh root@$i '/usr/zk/bin/zkServer.sh start'
done

echo ==========执行完成==========