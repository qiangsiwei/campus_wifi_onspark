# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 统计单用户HTTP总数量
if __name__ == "__main__":
	conf = SparkConf().setMaster('yarn-client') \
					  .setAppName('qiangsiwei') \
					  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	filename = "20130501"
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	line1 = sc.textFile("{0}/{1}/merge/{2}".format(hdfs_base,proj_base,filename), 1)
	data1 = line1.map(lambda x : (x.split(" ")[0], 1)) \
			.reduceByKey(add) \
			.map(lambda x : (x[1],x[0])) \
			.sortByKey()\
			.map(lambda x : '{0}\t{1}'.format(x[0],x[1]))
	output1 = data1.saveAsTextFile("{0}/{1}/stat_http/{2}".format(hdfs_base,proj_base,filename))
