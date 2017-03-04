# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# AP统计
# STEP 1
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
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile("{0}/{1}/move".format(hdfs_base,proj_base), 1)
	data1 = line1.map(lambda x : (x.split(" ")[1], int(x.split(" ")[4]))) \
			.reduceByKey(add) \
			.sortByKey() \
			.map(lambda x : x[0]+" "+str(x[1]))
	output1 = data1.saveAsTextFile("{0}/{1}/ap".format(hdfs_base,proj_base))

def step2():
	import fileinput
	map = {}
	for line in fileinput.input("_ap/ap.txt"):
		if any([line.split(" ")[0].split("-")[0]==e for e in ["FH","MH","XH",]]):
			map["-".join(line.split(" ")[0].split("-")[0:2])] = True
		else:
			map[line.split(" ")[0].split("-")[0]] = True
	fileinput.close()
	print len(map.keys())
	for k in sorted(map.keys()):
		print k

if __name__ == "__main__":
	# step1()
	step2()

