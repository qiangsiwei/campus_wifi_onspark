# -*-coding:utf-8-*-

import re
import gzip

def extract_login(date=''):
	_base = "/home/nfs/dataset/SJTU/wifi-syslog"
	_file = open("_login/login_{0}.txt".format(date),"w")
	for line in gzip.open("{0}/wifilog{1}.gz".format(_base,date)):
		try:
			if "<522008>" in line:
				username, MAC = re.findall('username=(.*?) ', line)[0], re.findall('MAC=(.*?) ', line)[0]
				_file.write(username+" "+MAC+"\n")
		except:
			continue

extract_login()