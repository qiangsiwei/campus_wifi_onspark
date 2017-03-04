# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 用户语义特征提取
def extract_keywords(line):
	import re
	import urllib
	import urlparse
	try:
		line, zh = line.encode("utf-8"), re.compile(u'[^\u4e00-\u9fa5]+')
		mac, up = line.strip().split(" ")[0], urlparse.parse_qs(urlparse.urlparse(line.strip().split(" ")[20][1:-1]).query)
		q, uq = "".join([str(up[e][0]) for e in sorted(up.keys())]), "".join([urllib.unquote(str(up[e][0])) for e in sorted(up.keys())])
		if q != uq:
			return (mac, zh.sub('',uq.decode("utf-8")).encode("utf-8"))
		else:
			return ("","")
	except:
		return ("","")

def step1():
	conf = SparkConf().setMaster('yarn-client') \
		  .setAppName('qiangsiwei') \
		  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	filename = "20130501"
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile("{0}/{1}/merge/{2}/".format(hdfs_base,proj_base,filename), 1)
	data1 = line1.map(lambda x : extract_keywords(x))\
			.filter(lambda x : x[0]!="" and x[1]!="") \
			.distinct() \
			.sortByKey() \
			.map(lambda x : x[0]+" "+x[1])
	output1 = data1.saveAsTextFile("{0}/{1}/feature/semantic/{2}".format(hdfs_base,proj_base,filename))

if __name__ == "__main__":
	step1()


