#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
    @Time    : 17/9/14 下午3:56
    @Author  : 孙玉强
    @Mail    : <sunyuqiang@kanzhun.com>
    @File    : config.py
    @Software: PyCharm
"""

from __future__ import (absolute_import, unicode_literals)



##### repl 需要all权限，至少replication 和select 权限需要查询

### way 1 grant all privileges on *.* to ‘repl'@'%' identified by 'repl';
### way 2 ：GRANT REPLICATION SLAVE,select  ON *.* to 'repl'@'192.168.1.%' identified by 'repl';
zabbix_host="172.21.1.12"
zabbix_port= 3306
zabbix_replication_user = "repluser"
zabbix_replication_pwd = "replpwd"
zabbix_replication_db = "zabbix"



# influxdb :
##[[udp]]
##  enabled = true
##bind-address = ":8099" # the bind address
##batch-size = 20480 # will flush if this many points get buffered
##batch-timeout = "1s" # will flush at least this often even if the batch-size is not reached
##batch-pending = 100 # number of batches that may be pending in memory
##read-buffer = 67108864 # (64m UDP read buffer size  推荐设置256M，把下面参数influx_batch_size=1024 批次 。提交一次。可以提高速度。
##bind-address = ":8089"
##database = "zabbix"
influx_host="127.0.0.1"
influx_port= 8089
influx_batch_size=128





#####other required
#####    pip install setproctitle
#####    pip install mysql-replication
#####    pip install influxdb==5.0.0
