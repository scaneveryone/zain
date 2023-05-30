#!/usr/bin/python
# -*- coding: UTF-8 -*-
import time

####进入influxdb的表记录
class history():
    def __init__(self):
        self.table="history"
        self.itemid=0
        self.itemname=None
        self.hostid=0
        self.hostname=None
        self.service_name=None
        self.value=None
        self.clock=None
        self.ns=None
        self.record_file=None
        self.record_pos=None



class item():
    def __init__(self):
        self.itemid=0
        self.itemname=None
        self.itemkey=None
        self.hostid=0
        self.hostname=None
        ###未处理前的name 在zabbix 的name
        self.item_prename=None
    def setItemName(self):
        self.itemname = self.item_prename
        if self.item_prename.rfind("$") > 0:
                key_start= self.itemkey.rfind("[")
                key_end = self.itemkey.rfind("]")
                key_list = list(self.itemkey[key_start + 1:key_end].split(","))
                for onekey in range(0, len(key_list)):
                    self.itemname = self.itemname.replace("$" + str(onekey + 1), key_list[onekey])
#class allitem():
#    set(itemid,item);





