# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 统计移动用户单日上网时间
def extract_dur(line):
	mac, dur = line.strip().split(" ")[0], int(line.strip().split(" ")[4])
	if mobile.has_key(mac):
		return (mac, dur)
	else:
		return ("", 0)

global mobile

if __name__ == "__main__":
	mobile = {}
	import fileinput
	for line in fileinput.input("_device/device_mobile.txt"):
		mobile[line.strip().split(" ")[0]] = True
	fileinput.close()
	conf = SparkConf().setMaster('yarn-client') \
		  .setAppName('qiangsiwei') \
		  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	filename = "20130501"
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile("{0}/{1}/move/{2}".format(hdfs_base,proj_base,filename), 1)
	data1 = line1.map(lambda x : extract_dur(x)) \
			.filter(lambda x : x[0]!="" and x[1]!=0) \
			.reduceByKey(add) \
			.map(lambda x : (x[1],x[0])) \
			.sortByKey(ascending=False) \
			.map(lambda x : x[1]+" "+str(x[0]))
	output1 = data1.saveAsTextFile("{0}/{1}/online/{2}".format(hdfs_base,proj_base,filename))
