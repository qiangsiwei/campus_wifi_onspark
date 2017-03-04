# -*- coding: utf-8 -*- 

# MAC与用户名对应
if __name__ == "__main__":
	import fileinput
	mob = {}
	for line in fileinput.input("_device/device_mobile.txt"):
		mac, cnt = line.strip().split(" ")[0], " ".join(line.strip().split(" ")[1:])
		mob[mac] = cnt
	fileinput.close()
	jac = {}
	for line in fileinput.input("_jaccount/jaccount.txt"):
		username, info = line.strip().split(" ")[-1], " ".join(line.strip().split(" ")[:-1])
		jac[username] = info
	fileinput.close()
	map = {}
	for line in fileinput.input("_jaccount/login.txt"):
		try:
			username, mac = line.strip().split(" ")[0], line.strip().split(" ")[1]
			if not map.has_key(mac):
				map[mac] = {}
			if not map[mac].has_key(username):
				map[mac][username] = 0
			map[mac][username] += 1
		except:
			continue
	fileinput.close()
	file = open("_jaccount/mac_user_info.txt","w")
	for mac,v in map.iteritems():
		username = sorted([{"username":e,"cnt":v[e]} for e in v], key=lambda x:x["cnt"], reverse=True)[0]["username"]
		if mob.has_key(mac) and jac.has_key(username):
			file.write(mac+"\t"+username+"\t"+mob[mac]+"\t"+jac[username]+"\n")
	file.close()
