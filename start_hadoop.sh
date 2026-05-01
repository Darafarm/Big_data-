#!/bin/bash

# start_hadoop.sh
# Auto-starts Hadoop on EC2 boot via crontab @reboot
#
# To register:
#   crontab -e
#   Add: @reboot /home/ubuntu/start_hadoop.sh


export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Wait 15 seconds for system to fully boot
sleep 15

# Start HDFS and YARN
start-dfs.sh
start-yarn.sh

echo "Hadoop started at $(date)" >> /home/ubuntu/hadoop_autostart.log
