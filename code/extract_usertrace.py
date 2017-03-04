# -*-coding:utf-8-*-

import re
import time
import gzip
import datetime
import fileinput

# 抽取用户移动轨迹与上网时段IP
def extract_trace(date=""):
	_list, _det = [], 4
	_base = "/home/nfs/dataset/SJTU/wifi-syslog"
	for line in gzip.open("{0}/wifilog{1}.gz".format(_base,date)):
		try:
			if "<522008>" in line:
				tspot, username, MAC, IP, AP = str(int(line.split(" ")[0])/1000-_det), re.findall('username=(.*?) ', line)[0], re.findall('MAC=(.*?) ', line)[0], re.findall('IP=(.*?) ', line)[0], re.findall('AP=(.*?) ', line)[0]
				_list.append({"MAC":MAC, "info":MAC+" "+tspot+" 1 "+username+" "+AP})
			elif "<522006>" in line:
				tspot, MAC, IP = str(int(line.split(" ")[0])/1000-_det), re.findall('MAC=(.*?) ', line)[0], re.findall('IP=(.*?) ', line)[0]
				if "111.186" in IP:
					_list.append({"MAC":MAC, "info":MAC+" "+tspot+" 2 "+IP})
			elif "<522005>" in line:
				tspot, MAC, IP = str(int(line.split(" ")[0])/1000-_det), re.findall('MAC=(.*?) ', line)[0], re.findall('IP=(.*?) ', line)[0]
				if "111.186" in IP:
					_list.append({"MAC":MAC, "info":MAC+" "+tspot+" 3 "+IP})
			elif "<500010>" in line:
				tspot, MAC, AP = str(int(line.split(" ")[0])/1000-_det), re.findall('Station (.*?),', line)[0], re.findall('AP (.*?),', line)[0]
				_list.append({"MAC":MAC, "info":MAC+" "+tspot+" 4 "+AP})
			elif any([code in line for code in ["<501102>","<501105>","<501106>","<501114>","<501080>"]]):
				tspot, MAC, AP = str(int(line.split(" ")[0])/1000-_det), re.findall('sta: (.*?) ', line)[0][:-1], "-".join(re.findall('AP (.*?) ', line)[-1].split("-")[2:])
				_list.append({"MAC":MAC, "info":MAC+" "+tspot+" 5 "+AP})
		except:
			continue
	_list = sorted(_list, key=lambda x: x["MAC"])
	_file = open("_movement/{0}".format(date),"w")
	for item in _list:
		part = item["info"].strip().split(" ")
		mac, time, code = part[0], int(part[1]), part[2]
		try:
			if code == "2":
				ip = part[3]
				if MAC == "" or mac != MAC:
					MAC, TIME, AP, IP = mac, time, "", ""
				else:
					if AP != "" and time-TIME>0:
						if IP == "":
							_file.write(MAC+' '+AP+' '+str(TIME)+' '+str(time)+' '+str(time-TIME)+'\n')
						else:
							_file.write(MAC+' '+AP+' '+str(TIME)+' '+str(time)+' '+str(time-TIME)+' '+IP+'\n')
						TIME = time
				if ip != IP:
					IP = ip
			if code == "3":
				ip = part[3]
				if MAC == "" or mac != MAC:
					MAC, TIME, AP, IP = mac, time, "", ""
				else:
					if ip == IP and AP != "" and time-TIME>0:
						_file.write(MAC+' '+AP+' '+str(TIME)+' '+str(time)+' '+str(time-TIME)+' '+IP+'\n')
						TIME = time
				if ip == IP:
					IP = ""
			if code == "1" or code == "4":
				ap = part[{"1":4,"4":3}[code]]
				if MAC == "" or mac != MAC:
					MAC, TIME, AP, IP = mac, time, ap, ""
				else:
					if AP != "" and time-TIME>0:
						if IP == "":
							_file.write(MAC+' '+AP+' '+str(TIME)+' '+str(time)+' '+str(time-TIME)+'\n')
						else:
							_file.write(MAC+' '+AP+' '+str(TIME)+' '+str(time)+' '+str(time-TIME)+' '+IP+'\n')
					TIME, AP = time, ap
			if code == "5":
				ap = part[3]
				if mac == MAC and AP != "":
					if time-TIME>0:
						if IP == "":
							_file.write(MAC+' '+AP+' '+str(TIME)+' '+str(time)+' '+str(time-TIME)+'\n')
						else:
							_file.write(MAC+' '+AP+' '+str(TIME)+' '+str(time)+' '+str(time-TIME)+' '+IP+'\n')
					MAC, TIME, AP, IP = "", 0, "", ""
		except:
			continue
	_file.close()
	return

# 统计并绘制用户人数随时间分布
def stat_trace(date=""):
	import numpy as np
	import matplotlib.pyplot as plt
	stat = {}
	for line in fileinput.input("_move/{0}".format(date)):
		part = line.strip().split(" ")
		if len(part) != 6:
			continue
		MAC, AP, st, ft, dr = part[0], part[1], int(part[2]), int(part[3]), int(part[4])
		t1, t2 = time.gmtime(st+8*3600), time.gmtime(ft+8*3600)
		hr1, hr2, tl = t1.tm_hour, t2.tm_hour, 0
		for h in range(hr1, hr2+1):
			if h == hr1:
				tl = min(ft, time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h+1).timetuple()) if h<23 else time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday+1, 0).timetuple()))-st
			if hr1 < h < hr2:
				tl = (time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h+1).timetuple()) if h<23 else time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday+1, 0).timetuple()))-time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h).timetuple())
			if hr1 != hr2 and h == hr2:
				tl = ft-max(st, time.mktime(datetime.datetime(t1.tm_year,t1.tm_mon,t1.tm_mday, h).timetuple()))
			if not stat.has_key(h):
				stat[h] = {}
			if not stat[h].has_key(MAC):
				stat[h][MAC] = 0
			stat[h][MAC] += tl
	fileinput.close()
	x = [i for i in xrange(24)]
	y = [len(stat[h].keys()) for h in stat]
	# y = [sum([stat[h][u] for u in stat[h]]) for h in stat]
	# y = [sum([stat[h][u] for u in stat[h]])/len(stat[h].keys()) for h in stat]
	fig, ax = plt.subplots()
	ax.plot(x, y, 'k--')
	ax.plot(x, y, 'ro')
	plt.show()

extract_trace()
stat_trace()
