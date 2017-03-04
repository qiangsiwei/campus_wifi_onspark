# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 设备识别
def extract_ua(line):
	try:
		mac, ua = line.strip().split(" ")[0], line.strip().split(" \"")[5][:-1].lower()
		if 'windows' in ua:
			return (mac+" 0", 1)
		elif any([e in ua for e in ['iphone','ipad','ipod','cfnetwork','android','apache-httpclient','windows phone','windows ce']]):
			return (mac+" 1", 1)
		else:
			return (mac+" N", 1)
	except:
		return ("", -1)

def step1():
	conf = SparkConf().setMaster('yarn-client') \
					  .setAppName('qiangsiwei') \
					  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	filename = "20130501"
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile('{0}/{1}/merge/{2}'.format(hdfs_base,proj_base,filename), 1)
	data1 = line1.map(lambda x : extract_ua(x)) \
			.filter(lambda x : x[0]!="") \
			.reduceByKey(add) \
			.map(lambda x : (x[0].split(" ")[0], x[0].split(" ")[1]+":"+str(x[1]))) \
			.groupByKey() \
			.map(lambda x : x[0]+" "+" ".join(x[1]))
	output1 = data1.saveAsTextFile("{0}/{1}/device/{2}".format(hdfs_base,proj_base,filename))

def step2():
	import fileinput
	map = {}
	for line in fileinput.input("_device/device.txt"):
		mac, cont = line.strip().split(" ")[0], line.strip().split(" ")[1]
		if not map.has_key(mac):
			map[mac] = [0,0]
		for it in cont.split(" "):
			if it.split(":")[0] == "0":
				map[mac][0] += int(it.split(":")[1])
			if it.split(":")[0] == "1":
				map[mac][1] += int(it.split(":")[1])
	fileinput.close()
	file1 = open("_device/device_mobile.txt","w")
	file2 = open("_device/device_nonmobile.txt","w")
	for k,v in map.iteritems():
		if v[1] >= 2*v[0] and v[1] >= 1000:
			file1.write(k+" "+str(v[0])+" "+str(v[1])+"\n")
		else:
			file2.write(k+" "+str(v[0])+" "+str(v[1])+"\n")
	file1.close()
	file2.close()

if __name__ == "__main__":
	# step1()
	step2()
