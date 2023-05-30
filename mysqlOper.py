#!/usr/bin/python
# -*- coding: UTF-8 -*-
import string
import MySQLdb
import traceback
import  socket
import re
import tools
import sys
import config
# 打开数据库连接

class OperDB:
   def __init__(self):
      self.conn = MySQLdb.connect(host=config.zabbix_host,user=config.zabbix_replication_user,passwd=config.zabbix_replication_pwd,db=config.zabbix_replication_db,charset="utf8" )
      self.cur=self.conn.cursor()


   def queryDB(self, querySql):
      sql=querySql

      try:
   # 执行SQL语句
         self.cur.execute(sql)
   # 获取所有记录列表
         results =  self.cur.fetchall()
         return results
      except:
            print "Error: unable to fecth data"
            traceback.print_exc()
# 关闭数据库连接

   def QueryItems(self, lastItems,itemid=0):
       if itemid==0:
            allitems = {}
            sql_getitems = "select itemid,items.`name` itemname,items.key_,hosts.hostid,hosts.`name` hostname,service_name  from items,hosts  where  hosts.hostid=items.hostid "
       else :
           allitems=lastItems
           sql_getitems = "select itemid,items.`name` itemname,items.key_,hosts.hostid,hosts.`name` hostname,service_name  from items,hosts  where  hosts.hostid=items.hostid and itemid=%s"%itemid
           print sql_getitems
       results = self.queryDB(sql_getitems)
       for row in results:
           aitem = tools.item()
           aitem.itemid = row[0]
           aitem.item_prename = row[1]
           aitem.itemkey = row[2]
           aitem.hostid = row[3]
           aitem.hostname = row[4]
           aitem.service_name = row[5]
           aitem.setItemName()
           allitems[aitem.itemid] = aitem
           if len(allitems) % 10000 == 0:
               print "load data from zabbix ..%s"%len(allitems)
               sys.stdout.flush()
       return allitems
   def __del__(self):
      self.cur.close()
      self.conn.close()
      print "closed db connect is ok....."
