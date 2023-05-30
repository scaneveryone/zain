#!/usr/bin/python
# -*- coding: utf-8 -*
import os
import time
import urllib2

from influxdb import InfluxDBClient
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


from influxdb import InfluxDBClient
from influxdb import SeriesHelper
from datetime import datetime




mysql_settings = {'host': config.zabbix_host, 'port': config.zabbix_port, 'user': config.zabbix_replication_user, 'passwd': config.zabbix_replication_pwd}
record_file = None
record_pos = 0
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

myclient = InfluxDBClient(host=config.influx_host, use_udp=True, udp_port=config.influx_port)

class MySeriesHelper(SeriesHelper):
        """Instantiate SeriesHelper to write points to the backend."""

        class Meta:
            """Meta class stores time series helper configuration."""

            # The client should be an instance of InfluxDBClient.
            client = myclient

            # The series name must be a string. Add dependent fields/tags
            # in curly brackets.
            series_name = 'history'

            # Defines all the fields in this time series.
            fields = ['value']

            # Defines all the tags for the series.
            tags = ['host', 'itemname']

            # Defines the number of data points to store prior to writing
            # on the wire.
            bulk_size = 8192

            # autocommit must be set to True when using bulk_size
            autocommit = True



stream = BinLogStreamReader(connection_settings=mysql_settings, server_id=200, blocking=True,
                            only_schemas=["zabbix"],
                            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, RotateEvent]
                            ,resume_stream=True
                            ,log_file=record_file, log_pos=record_pos)
print "From:starting engine ..From last record>>>>>>>>>>>>>>>>>>>>>>file:%s,pos:%s" % (record_file, record_pos)
sys.stdout.flush()

allitems = {}
myoper = mysqlOper.OperDB()
allitems = myoper.QueryItems(allitems)
myoper.__del__()


insertrow_cnt=0
insertevent_cnt=0
last_checkpoint_time=datetime.now()

for binlogevent in stream:
    if isinstance(binlogevent,RotateEvent):
        record_file= binlogevent.next_binlog
        record_pos=binlogevent.position
        print "From:checkpoint_Rotate....file:%s,pos:%s" % (record_file, record_pos)
        sys.stdout.flush()
    if isinstance(binlogevent, WriteRowsEvent) and binlogevent.schema=='zabbix' and binlogevent.table=='history' :
            record_pos = binlogevent.packet.log_pos
            for row in binlogevent.rows:
                insertrow_cnt=insertrow_cnt+1
                insertevent_cnt = insertevent_cnt + 1
                vals = row["values"]
                inArow=tools.history()
                inArow.itemid=vals["itemid"]
                inArow.value = vals["value"]
                inArow.clock = vals["clock"]
                inArow.ns=vals["ns"]
                try:
                    inArow.hostname=allitems[inArow.itemid].hostname
                    inArow.itemname = allitems[inArow.itemid].itemname
                    inArow.hostid = allitems[inArow.itemid].hostid
                except:
                    myoper2 = mysqlOper.OperDB()
                    allitems = myoper2.QueryItems(allitems, inArow.itemid)
                    myoper2.__del__()
                    print "reload from db...............%s" %inArow.itemid
                    inArow.hostname = allitems[inArow.itemid].hostname
                    inArow.itemname = allitems[inArow.itemid].itemname
                    inArow.hostid = allitems[inArow.itemid].hostid
                    print inArow.itemname

                MySeriesHelper(host=inArow.hostname, itemname=inArow.itemname, value=inArow.value,
                                   time=inArow.clock * 1000000000 + inArow.ns)
                if insertrow_cnt%512 == 0 or insertrow_cnt > 512:
                    MySeriesHelper.commit()
                    #os.system('echo  ' + record_file + "  " + str(record_pos) + ' >master.info')
                    insertrow_cnt = 0
            if insertevent_cnt%20480==0 or insertevent_cnt>20480 or (datetime.now()-last_checkpoint_time).seconds>3:
                MySeriesHelper.commit()
                os.system('echo  ' + record_file + "  " + str(record_pos) + ' >master.info')
                insertrow_cnt=0
                insertevent_cnt=0
                last_checkpoint_time = datetime.now()
                os.system('echo  ' + record_file + "  " + str(record_pos) + ' >master.info')
                sys.stdout.flush()
                print "From:checkpoint...file:%s,pos:%s" % (record_file, record_pos)


###stream.close()
