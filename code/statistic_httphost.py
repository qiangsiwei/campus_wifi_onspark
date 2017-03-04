# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 统计HTTP host
def extract_host(line):
	import re
	dg = re.compile(u'[0-9]+')
	try:
		hcl = ".".join(filter(lambda x:dg.sub('',x.split(":")[0])!="",line.strip().split(" ")[20][1:-1].split(".")))
		if 5 <= len(hcl) < 30:
			return (hcl)
		else:
			return ("")
	except:
		return ("")

def step1():
	conf = SparkConf().setMaster('yarn-client') \
		  .setAppName('qiangsiwei') \
		  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile("{0}/{1}/http/*".format(hdfs_base,proj_base), 1)
	data1 = line1.map(lambda x : extract_host(x)) \
			.filter(lambda x : x!="") \
			.map(lambda x : (x,1)) \
			.reduceByKey(add) \
			.map(lambda x : (x[1],x[0])) \
			.sortByKey(ascending=False) \
			.map(lambda x : x[1]+" "+str(x[0]))
	output1 = data1.saveAsTextFile("{0}/{1}/host/count".format(hdfs_base,proj_base))

def step2():
	import fileinput
	file = open("_host/host_top.txt","w")
	for line in fileinput.input("_host/host.txt"):
		host, count = line.strip().split(" ")[0], int(line.strip().split(" ")[1])
		if count >= 5000:
			file.write(host+"\n")
	fileinput.close()
	file.close()

if __name__ == "__main__":
	# step1()
	step2()


