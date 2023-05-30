#!/usr/bin/python
# -*- coding: utf-8 -*
import traceback
from Queue import Empty

from influxdb import InfluxDBClient
from influxdb import SeriesHelper
import sys
import setproctitle
import config

from multiprocessing import Process, Queue, Lock
import time
reload(sys)
sys.setdefaultencoding("utf-8")
import  os

def zainFlux(q):
    setproctitle.setproctitle("writeToInflux-zain")
    time.sleep(1)
    last_record_file=None
    last_record_pos=None

    record_file =None
    record_pos = None

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
            tags = ['host', 'itemname','service_name']

            # Defines the number of data points to store prior to writing
            # on the wire.
            bulk_size = config.influx_batch_size*2

            # autocommit must be set to True when using bulk_size
            autocommit = True
    # MySeriesHelper(server_name='as.east-1',itemname='9', some_stat=159, other_stat=10,time=1545791588720328960)

    while True:
                inArow =None
                if not q.empty():
                  try:
                    for k in range(config.influx_batch_size):
                           inArow = q.get()
                           v_service_name=inArow.service_name
                           #print v_service_name
                           if v_service_name == 'None' or v_service_name is None or len(v_service_name) <1:
                             v_service_name=''
                           record_file = inArow.record_file
                           record_pos = inArow.record_pos
                           MySeriesHelper(host=inArow.hostname,service_name=v_service_name, itemname=inArow.itemname, value=float(inArow.value),time=inArow.clock*1000000000+inArow.ns)
                    MySeriesHelper.commit()
                  except Empty,e:
                    print time.strftime("%Y%m%d%H-%M-%S",
                                          time.localtime()) + ":To:INFO:GET NOT Fill 512 data into influxdb pid:" + str(
                          os.getpid()) + "............last_record_file:%s,last_record_pos:%s... ..." % (
                            last_record_file, last_record_pos)
                    sys.stdout.flush()
                    MySeriesHelper.commit()
                    time.sleep(0.05)
                  except Exception,e:
                      print "error x2"
                      sys.stdout.flush()
                      print 'e.message:\t', e.message
                      print 'traceback.print_exc():';
                      #traceback.print_exc()
                      print 'traceback.format_exc():\n%s' % traceback.format_exc()
                      print time.strftime("%Y%m%d%H-%M-%S", time.localtime()) + ":To:ERROR_FAILED_ERROR ERR pid:" + str(os.getpid()) + "............last_record_file:%s,last_record_pos:%s... ..." % (
                            last_record_file, last_record_pos)
                      sys.stdout.flush()
                      time.sleep(0.05)
                      ####忽略错误继续前进
                      continue;
                      ###在错误的地方趴下去
                      #sys.exit(-1)
                else:
                    ###下次记录可能的地址。下次记录。这次记录上次的。防止误丢
                    if last_record_pos < record_pos or last_record_file<record_file:
                        last_record_file = record_file
                        last_record_pos = record_pos
                    if last_record_pos !=None and last_record_file!=None:
                        os.system('echo  ' + last_record_file + "  " + str(last_record_pos)+'  '+time.strftime("%Y%m%d%H-%M-%S", time.localtime()) + ' >master.info')
                    time.sleep(0.05)
                    print time.strftime("%Y%m%d%H-%M-%S", time.localtime())+":To:INFO:None data into influxdb pid:"+str(os.getpid())+"............last_record_file:%s,last_record_pos:%s... ..."%(last_record_file,last_record_pos)
                    sys.stdout.flush()
            # To inspect the JSON which will be written, call _json_body_():

