#!/bin/sh

zookeeper=/Users/korn/Documents/UChicago/MasterProject/zookeeper-3.1.0
classpath=.:$zookeeper/build/classes
lib=$zookeeper/lib
for j in `ls $lib/*.jar`; do
  classpath=$classpath:$j
done
export CLASSPATH=$CLASSPATH:$classpath
export PATH=$PATH:bin/

java -Dsun.rmi.dgc.cleanInterval=10000 -Dsun.rmi.dgc.server.gcInterval=10000 -Dlog4j.configuration=mc_log.properties -Dzookeepertest.dir=WORKING_DIR mc.zookeeper.ZooKeeperTestRunner ./mc.conf

