#!/bin/bash

SPARK_HOME=/usr/hdp/2.4.2.0-258/spark

#clear folder with previous run results
runuser -l hdfs -c "hdfs dfs -rm -R -skipTrash /tmp/tags" && runuser -l hdfs -c "hdfs dfs -rm -R -skipTrash /tmp/users"

#run spark job
$SPARK_HOME/bin/spark-submit --class com.epam.spark.App --master local[1] spark_hw_1-1.0.jar keywordsApp local[2] hdfs://sandbox.hortonworks.com:8020/mr-data/dic/user.profile.tags.us.txt hdfs://sandbox.hortonworks.com:8020/mr-data/dic/city.us.txt hdfs://sandbox.hortonworks.com:8020/mr-data/test_datase2.txt 10 hdfs://sandbox.hortonworks.com:8020/tmp/tags hdfs://sandbox.hortonworks.com:8020/tmp/users