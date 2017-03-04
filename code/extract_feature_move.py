# -*- coding: utf-8 -*- 

import sys
from operator import add
from pyspark import SparkConf
from pyspark import SparkContext

# 用户位置特征提取
def extract_pos(line):
	import time
	import datetime
	try:
		part = line.strip().split(" ")
		mac, ap, st, ft = part[0], "-".join(part[1].split("-")[0:2]) if any([part[1].split("-")[0]==e for e in ["FH","MH","XH"]]) else part[1].split("-")[0], int(part[2]), int(part[3])
		t1, t2 = time.gmtime(st+8*3600), time.gmtime(ft+8*3600)
		hr1, hr2, yd, wd, tslice = t1.tm_hour, t2.tm_hour, t1.tm_yday, 1 if 0<=t1.tm_wday<=4 else 0, []
		for h in range(hr1, hr2+1):
			if h == hr1:
				tslice.append([wd,h,abs(st-min(ft, time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h+1).timetuple()) if h<23 else time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday+1, 0).timetuple())))])
			if hr1 < h < hr2:
				tslice.append([wd,h,abs(time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h).timetuple())-(time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h+1).timetuple()) if h<23 else time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday+1, 0).timetuple())))])
			if hr1 != hr2 and h == hr2:
				tslice.append([wd,h,abs(max(st, time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h).timetuple()))-ft)])
		return ((mac, ap, yd), tslice)
	except:
		return (("", ""), [])

def step1():
	conf = SparkConf().setMaster('yarn-client') \
			  .setAppName('qiangsiwei') \
			  .set('spark.driver.maxResultSize', "48g")
	sc = SparkContext(conf = conf)
	proj_base = "wifi"
	hdfs_base = "hdfs://namenode.omnilab.sjtu.edu.cn/user/qiangsiwei"
	def f(x): return x
	line1 = sc.textFile("{0}/{1}/move".format(hdfs_base,proj_base), 1)
	data1 = line1.map(lambda x : extract_pos(x)) \
			.filter(lambda x : x[0]!="" and x[1]!="") \
			.flatMapValues(f) \
			.map(lambda x : ((x[0][0]+" "+x[0][1]+" "+str(x[0][2])+" "+str(x[1][0])+" "+str(x[1][1])),x[1][2])) \
			.reduceByKey(add) \
			.filter(lambda x : x[1]>=5*60) \
			.map(lambda x : ((x[0].split(" ")[0]+" "+x[0].split(" ")[1]+" "+x[0].split(" ")[3]+" "+x[0].split(" ")[4]),1)) \
			.reduceByKey(add) \
			.sortByKey() \
			.map(lambda x : x[0]+" "+str(x[1]))
	output1 = data1.saveAsTextFile("{0}/{1}/feature/move".format(hdfs_base,proj_base))

def step2():
	import fileinput
	apmap = {}
	for line in fileinput.input("_ap/ap.txt"):
		apmap["-".join(line.strip().split(" ")[0].split("-")[0:2]) if any([line.strip().split(" ")[0].split("-")[0]==e for e in ["FH","MH","XH"]]) else line.strip().split(" ")[0].split("-")[0]] = True
	fileinput.close()
	jac = {}
	for line in fileinput.input("_jaccount/mac_user_info.txt"):
		mac = line.strip().split("\t")[0]
		jac[mac] = True
	fileinput.close()
	map, feature = {}, {}
	for line in fileinput.input("_feature/move.txt"):
		mac, ap, wd, h, cnt = line.strip().split(" ")[0], line.strip().split(" ")[1], int(line.strip().split(" ")[2]), int(line.strip().split(" ")[3]), int(line.strip().split(" ")[-1])
		map[mac] = cnt if not map.has_key(mac) else map[mac]+cnt
		if not feature.has_key(mac):
			feature[mac] = {0:{h:{} for h in xrange(24)},1:{h:{} for h in xrange(24)}}
		feature[mac][wd][h][ap] = cnt
	fileinput.close()
	file = open("_feature/move_vector.txt","w")
	for x in filter(lambda x:jac.has_key(x["mac"]) and x["cnt"]>=8*31, sorted([{"mac":k,"cnt":map[k]} for k in map.keys()], key=lambda x:x["cnt"], reverse=True)):
		file.write(x["mac"])
		f = feature[x["mac"]]
		for wd in range(0,2):
			for h in range(8,22):
				for ap in apmap.keys():
					if f[wd][h].has_key(ap):
						file.write(" "+str(f[wd][h][ap]))
					else:
						file.write(" 0")
		file.write("\n")

if __name__ == "__main__":
	# step1()
	step2()
	
