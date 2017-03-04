# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 用户上网特征提取
def extract_host(line):
	import re
	dg = re.compile(u'[0-9]+')
	try:
		mac, hcl = line.strip().split(" ")[0], ".".join(filter(lambda x:dg.sub('',x.split(":")[0])!="",line.strip().split(" ")[22][1:-1].split(".")))
		if hosts.has_key(hcl):
			return (mac, hosts[hcl])
		else:
			return ("", 0)
	except:
		return ("", 0)

def feature_host(list):
	map = {}
	for it in list:
		map[it[0]] = it[1]
	return " ".join([str(map[i]) if map.has_key(i) else "0" for i in range(1,len(hosts.keys())+1)])

global hosts

def step1():
	import fileinput
	hosts, sta = {}, 0
	for line in fileinput.input("_host/host_top.txt"):
		sta += 1
		hosts[line.strip()] = sta
	fileinput.close()
	conf = SparkConf().setMaster('yarn-client') \
		  .setAppName('qiangsiwei') \
		  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	filename = "20130501"
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile("{0}/{1}/merge/{2}".format(hdfs_base,proj_base,filename), 1)
	data1 = line1.map(lambda x : extract_host(x)) \
			.filter(lambda x : x[0]!="" and x[1]!=0) \
			.map(lambda x : ((x[0],x[1]),1)) \
			.reduceByKey(add) \
			.map(lambda x : (x[0][0],(x[0][1],x[1]))) \
			.groupByKey() \
			.map(lambda x : (x[0]+" "+feature_host(x[1])))
	output1 = data1.saveAsTextFile("{0}/{1}/cyber/{2}".format(hdfs_base,proj_base,filename))

def feature_agg(list):
	feature = [0]*len(hosts)
	for it in list:
		feature = [a+b for a,b in zip(feature,it)]
	return (" ".join([str(i) for i in feature]))

def step2():
	hosts = {}
	import fileinput
	sta = 0
	for line in fileinput.input("_host/host_top.txt"):
		sta += 1
		hosts[line.strip()] = sta
	fileinput.close()
	conf = SparkConf().setMaster('yarn-client') \
		  .setAppName('qiangsiwei') \
		  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile("{0}/{1}/cyber/*".format(hdfs_base,proj_base), 1)
	data1 = line1.map(lambda x : (x.strip().split(" ")[0], [int(i) for i in x.strip().split(" ")[1:]])) \
			.groupByKey() \
			.map(lambda x : (x[0]+" "+feature_agg(x[1])))
	output1 = data1.saveAsTextFile("{0}/{1}/feature/cyber/")

if __name__ == "__main__":
	# step1()
	step2()
