
#!/bin/bash
nodeNum=5
processListInNode=("0,1,2" "0,1,2" "2" "2" "2")

processName=("NameNode" "Master" "Worker")
processClass=("hdfs" "spark" "spark")
processScript=("/usr/lib/xdata/hdfs/sbin/hadoop-daemons.sh --config /usr/lib/xdata/hdfs/etc/hadoop --hostnames `hostname` --script /usr/lib/xdata/hdfs/sbin/hdfs start namenode" "/usr/lib/xdata/spark/sbin/stop-master.sh && /usr/lib/xdata/spark/sbin/start-master.sh" "/usr/lib/xdata/spark/sbin/stop-slave.sh && /usr/lib/xdata/spark/sbin/start-slave.sh 1 spark://node1:7077,node2:7077")

nodeId=`hostname |tr -d "node"`
nodeIndex=`expr $nodeId - 1`
OLD_IFS="$IFS" 
IFS="," 
processList=(${processListInNode[$nodeIndex]}) 
IFS="$OLD_IFS" 
for processIndex in ${processList[@]}
do 
	processId=`ps -ef | grep ${processName[$processIndex]}| grep ${processClass[$processIndex]} | awk '{print $2}'`
	if [ "$processId" =  "" ]; then
		echo `date` == `hostname` ==
		echo "[${processClass[$processIndex]} ${processName[$processIndex]}] is restart!"
		echo ${processScript[$processIndex]} | awk -F "&&" '{for(i=1;i<=NF;i++){system($i)}}'
	#else
		#echo stop ${processClass[$processIndex]} ${processName[$processIndex]}
		#kill $processId
		#echo "[${processClass[$processIndex]} ${processName[$processIndex]}] is alive!"
	fi
done

