#!/bin/bash

BASE_DIR=/home/s/sixuhu
export JAVA_HOME=$BASE_DIR/java
export HADOOP_HOME=$BASE_DIR/hadoop
export SPARK_HOME=$BASE_DIR/spark
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$SPARK_HOME/bin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
export PATH=~/spark/bin:$PATH

export PATH=$HOME/.local/bin:$PATH
export PATH=$BASE_DIR/maven/bin:$PATH
