#!/bin/bash
ps -ef|grep zain|grep -v grep|awk '{print $2}' |xargs kill -9 
