echo ==========集群节点==========

for i in master slave1 slave2
do
       echo ==========$i==========
       ssh root@$i '/usr/zk/bin/zkServer.sh stop'
done

echo ==========执行结束==========
