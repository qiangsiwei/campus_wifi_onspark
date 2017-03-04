# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 给HTTP打MAC和AP标签
def extract_move(line):
	try:
		part = line.strip().split(" ")
		MAC, AP, st, ft, IP = part[0], part[1], int(part[2]), int(part[3]), part[5] 
		return (IP, [st, ft, MAC+" "+AP])
	except:
		return ("", [])

def extract_http(line):
	try:
		part = line.strip().split(" ")
		IP, tm, host = part[0], int(part[9][0:10]), part[21]
		if host != "\"N/A\"":
			return (IP, [tm, line.strip()])
		else:
			return ("", [])
	except:
		return ("", [])

if __name__ == "__main__":
	conf = SparkConf().setMaster('yarn-client') \
					  .setAppName('qiangsiwei') \
					  .set('spark.driver.maxResultSize', "8g")
	sc = SparkContext(conf = conf)
	delta = 10*60
	filename = "20130501"
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile('{0}/{1}/{2}/{3}'.format(hdfs_base,proj_base,'move',filename), 1)
	data1 = line1.map(lambda x : extract_move(x)) \
			.filter(lambda x : x[0]!="")
	line2 = sc.textFile('{0}/{1}/{2}/{3}'.format(hdfs_base,proj_base,'http',filename), 1)
	data2 = line2.map(lambda x : extract_http(x)) \
			.filter(lambda x : x[0]!="")
	data = data1.join(data2)
	data = data.filter(lambda x : x[1][0][0]-delta <= x[1][1][0] and x[1][0][1]+delta >= x[1][1][0]) \
			.map(lambda x : (x[1][0][2].split(" ")[0]+" "+str(x[1][1][0]), x[1][0][2]+" "+x[1][1][1])) \
			.distinct() \
			.sortByKey() \
			.map(lambda x : x[1])
	output = data.saveAsTextFile("{0}/{1}/{2}".format(proj_base,'merge',filename))
