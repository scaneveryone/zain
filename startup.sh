#!/bin/bash
cd /opt/zain-v2
export WORKDIR=/opt/zain-v2
ps -ef|grep zain|grep -v grep|awk '{print $2}' |xargs kill -9 
sleep 1
echo "start process......"
/bin/nohup /bin/python zainMasterOPS.py >>${WORKDIR}/running.log 2>&1 &
