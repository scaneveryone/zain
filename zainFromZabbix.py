#!/usr/bin/python
# -*- coding: utf-8 -*
import os
import time
import urllib2

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
import setproctitle
import sys
import json

import config
import mysqlOper
from multiprocessing import Process, Queue, Lock
import tools
from urllib2 import quote
reload(sys)
sys.setdefaultencoding("utf-8")
import os
mysql_settings = {'host': config.zabbix_host, 'port': config.zabbix_port, 'user': config.zabbix_replication_user, 'passwd': config.zabbix_replication_pwd}
def   readFromZabbix(q):
            setproctitle.setproctitle("readFromZabbix-zain")

            record_file=None
            record_pos=0
            try:
                f = open('master.info', 'r')
                line = f.read()
                ops_record = line.split(" ")
                record_file = ops_record[0].strip()
                record_pos = int(ops_record[1].strip())
                if record_pos==0:
                    record_pos=None
                    resume_stream=False
            finally:
                if f:
                    f.close()

            stream = BinLogStreamReader(
                                        connection_settings=mysql_settings,
                                        server_id=1242,
                                        blocking=True,
                                        only_schemas=["zabbix"],
                                        only_tables = ["history", "history_uint"],
                                        only_events=[WriteRowsEvent],
                                        resume_stream=True,
                                        log_file=record_file,
                                        log_pos=record_pos
                                        )
            print time.strftime("%Y%m%d%H-%M-%S", time.localtime())+":From:INFO:starting engine ..From last record>>>>>>>>>>>>>>>>>>>>>>file:%s,pos:%s" % (record_file, record_pos)
            sys.stdout.flush()
            ######################load itemname hostname
            allitems={}
            myoper=mysqlOper.OperDB()
            allitems =myoper.QueryItems(allitems)
            myoper.__del__()
            #############load over

            for binlogevent in stream:
                record_pos = stream.log_pos
                record_file=stream.log_file
                if q.qsize()>40960:
                    time.sleep(1)
                for row in binlogevent.rows:
                            vals = row["values"]
                            inArow=tools.history()
                            inArow.itemid=vals["itemid"]
                            inArow.value = vals["value"]
                            inArow.clock = vals["clock"]
                            inArow.ns=vals["ns"]
                            inArow.record_file = record_file
                            inArow.record_pos = record_pos
                            try:
                                inArow.hostname=allitems[inArow.itemid].hostname
                                inArow.itemname = allitems[inArow.itemid].itemname
                                inArow.hostid = allitems[inArow.itemid].hostid
                                inArow.service_name = allitems[inArow.itemid].service_name
                            except:
                                myoper2 = mysqlOper.OperDB()
                                allitems = myoper2.QueryItems(allitems, inArow.itemid)
                                myoper2.__del__()
                                print time.strftime("%Y%m%d%H-%M-%S", time.localtime())+":From:INFO:reload from db.......itemname:%s........itemid:%s" %(inArow.itemname,inArow.itemid)
                                sys.stdout.flush()
                                #inArow.hostname = allitems[inArow.itemid].hostname
                                #inArow.itemname = allitems[inArow.itemid].itemname
                                #inArow.service_name = allitems[inArow.itemid].service_name
                                #inArow.hostid = allitems[inArow.itemid].hostid
                            q.put(inArow)
