#!/usr/bin/python
# -*- coding: utf-8 -*
'''
required:
    pip install setproctitle
    pip install mysql-replication
    pip install influxdb==5.0.0
'''
import zainFromZabbix
import zainFlux
import  sys
import setproctitle
reload (sys)
sys.setdefaultencoding('utf-8')
from multiprocessing import Process, Queue, Lock
import os, time


def clockqsize(q):
    setproctitle.setproctitle("clockqsize-zain")
    while True:
        print time.strftime("%Y%m%d%H-%M-%S", time.localtime())+":CLOCK:INFO:消息队列长度：", q.qsize()
        time.sleep(10)
        sys.stdout.flush()
if __name__ == "__main__":
    print time.strftime("%Y%m%d%H-%M-%S", time.localtime()) + ':MASTER:initing...starting.......'
    sys.stdout.flush()
    #父进程创建queue，并共享给各个子进程
    q= Queue()
    pr = Process(target=zainFromZabbix.readFromZabbix, args=(q,))
    time.sleep(1)
    pw = Process(target = zainFlux.zainFlux, args=(q,))
    clockq = Process(target=clockqsize, args=(q,))

    pw.daemon=True
    pr.daemon=True
    #clockq.daemon=True

    clockq.start()
    pw.start()
    pr.start()
    clockq.join()


    ####pw.join()
    ##pr.join()
    ####等待clockq结束。不然全部结束了


    print time.strftime("%Y%m%d%H-%M-%S", time.localtime())+':MASTER:workMaster finish.'