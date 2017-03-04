#-*-coding:utf-8-*-

import gzip
import fileinput
import numpy as np

def meta_show():
	map = {}
	for line in fileinput.input("_jaccount/mac_user_info.txt"):
		val = line.strip().split("\t")[3].split(" ")[1]
		# val = line.strip().split("\t")[3].split(" ")[2]
		# val = line.strip().split("\t")[3].split(" ")[3]
		map[val] = 1 if not map.has_key(val) else map[val]+1
	fileinput.close()
	for it in sorted(map.keys()):
		print it, map[it]
	# 女性 398
	# 男性 683
	# 教职员 34
	# 研究生 121
	# 本科生 926
	# 无 34
	# 药学院 3
	# 数学系 39
	# 医学院 41
	# 人文学院 6
	# 软件学院 7
	# 致远学院 19
	# 密西根学院 7
	# 微电子学院 13
	# 外国语学院 30
	# 凯原法学院 36
	# 化学化工学院 14
	# 物理与天文系 27
	# 上海中医药大学 2
	# 媒体与设计学院 20
	# 农业与生物学院 37
	# 生物医学工程学院 14
	# 生命科学技术学院 18
	# 信息安全工程学院 23
	# 系统生物医学研究院 2
	# 国际与公共事务学院 9
	# 环境科学与工程学院 16
	# 材料科学与工程学院 58
	# 安泰经济与管理学院 80
	# 机械与动力工程学院 145
	# 微纳米科学技术研究院 2
	# 巴黎高科卓越工程师学院 12
	# 船舶海洋与建筑工程学院 68
	# 电子信息与电气工程学院 291
	# 航空航天学院(空天科学技术研究院) 8

def prediction_move():
	mac_map, X, y = {}, [], []
	for line in fileinput.input("_jaccount/mac_user_info.txt"):
		# mac, val = line.strip().split("\t")[0], line.strip().split("\t")[3].split(" ")[1]
		# if val in ["男性","女性"]:
		# 	mac_map[mac] = 0 if val == "男性" else 1
		mac, val = line.strip().split("\t")[0], line.strip().split("\t")[3].split(" ")[2]
		if val in ["教职员","研究生","本科生"]:
			mac_map[mac] = 0 if val == "教职员" else 1 if val == "研究生" else 2
	fileinput.close()
	for line in gzip.open("_feature/move_vector.txt.gz"):
		mac = line.strip().split(" ")[0]
		if mac_map.has_key(mac):
			X.append([int(i) for i in line.strip().split(" ")[1:]])
			y.append(mac_map[mac])
	from sklearn import pipeline
	from sklearn import linear_model
	from sklearn import svm
	from sklearn import tree
	from sklearn import ensemble
	from sklearn.cross_validation import KFold
	from sklearn.cross_validation import LeaveOneOut
	total, right = 0, 0
	X, y = np.array(X), np.array(y)
	loo = LeaveOneOut(len(X))
	print len(y)
	for train, test in loo:
		clf = pipeline.Pipeline([
			('feature_selection', linear_model.LogisticRegression(penalty='l1')),
			# ('feature_selection', svm.LinearSVC(penalty="l1",dual=False)),
			# ('classification', svm.SVC())
			# ('classification', svm.LinearSVC())
			# ('classification', tree.DecisionTreeClassifier())
			('classification', ensemble.RandomForestClassifier())
			# ('classification', ensemble.GradientBoostingClassifier())
			])
		clf = clf.fit(X[train],y[train])
		r = clf.predict(X[test])[0]
		print r, y[test]
		if r == y[test]:
			right += 1
		total += 1
	print float(right)/total

def prediction_semantic():
	mac_map, X, y = {}, [], []
	for line in fileinput.input("_jaccount/mac_user_info.txt"):
		mac, val = line.strip().split("\t")[0], line.strip().split("\t")[3].split(" ")[1]
		if val in ["男性","女性"]:
			mac_map[mac] = 0 if val == "男性" else 1
		# mac, val = line.strip().split("\t")[0], line.strip().split("\t")[3].split(" ")[2]
		# if val in ["教职员","研究生","本科生"]:
		# 	mac_map[mac] = 0 if val == "教职员" else 1 if val == "研究生" else 2
	fileinput.close()
	for line in gzip.open("_feature/semantic_vector.txt.gz"):
		mac, tot = line.strip().split(" ")[0], sum([1 if int(c)>0 else 0 for c in line.strip().split(" ")[1:]])
		if mac_map.has_key(mac) and tot >= 10:
			X.append([int(i) for i in line.strip().split(" ")[1:]])
			y.append(mac_map[mac])
	fileinput.close()
	from sklearn import pipeline
	from sklearn import linear_model
	from sklearn import svm
	from sklearn import tree
	from sklearn import ensemble
	from sklearn.cross_validation import KFold
	from sklearn.cross_validation import LeaveOneOut
	total, right = 0, 0
	X, y = np.array(X), np.array(y)
	loo = LeaveOneOut(len(X))
	print len(y)
	for train, test in loo:
		clf = pipeline.Pipeline([
			('feature_selection', linear_model.LogisticRegression(penalty='l1')),
			# ('feature_selection', svm.LinearSVC(penalty="l1",dual=False)),
			# ('classification', svm.SVC())
			# ('classification', svm.LinearSVC())
			# ('classification', tree.DecisionTreeClassifier())
			('classification', ensemble.RandomForestClassifier())
			# ('classification', ensemble.GradientBoostingClassifier())
			])
		clf = clf.fit(X[train],y[train])
		r = clf.predict(X[test])[0]
		print r, y[test]
		if r == y[test]:
			right += 1
		total += 1
	print float(right)/total

if __name__ == "__main__":
	meta_show()
	# prediction_move()
	# prediction_semantic()
