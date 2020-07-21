#-*- coding: UTF-8 -*-
import sys
import importlib
import time
import urllib
import requests
import numpy as np
import lxml
import re
import json
import threading
import random
import http.client
import MySQLdb
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from urllib import request
from lxml import etree
from urllib.parse import urlencode
from fake_useragent import UserAgent

ua = UserAgent()

importlib.reload(sys)

def iplist():
    tmp = open('verified.txt', 'r')
    lock = threading.Lock()
    ip_list = []
    requestHeader = {
        'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36"}
    myurl = 'http://www.baidu.com/'
    while True:
        lock.acquire()
        ll = tmp.readline().strip()
        lock.release()
        if len(ll) == 0:
            break
        line = ll.split('|')
        ip = line[1]
        port = line[2]
        res_time = line[6]
        cos_time = line[7]
        # if float(res_time[:-1]) < 0.1 :
        if float(res_time[:-1]) < 0.1 and cos_time[-1] != '秒':
            # if float(res_time[:-1]) < 0.1 and cos_time[-1] == "天":
            try:
                ip_list.append(ip + ":" + port)
            except:
                pass

    tmp.close()
    return ip_list

IPlIST=iplist()

def getiplist():
    tmp = open('verified.txt', 'r')
    lock = threading.Lock()
    ip_list = []
    requestHeader = {
        'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36"}
    myurl = 'http://www.baidu.com/'
    while True:
        lock.acquire()
        ll = tmp.readline().strip()
        lock.release()
        if len(ll) == 0:
            break
        line = ll.split('|')
        ip = line[1]
        port = line[2]
        res_time = line[6]
        cos_time = line[7]
        # if float(res_time[:-1]) < 0.1 :
        if float(res_time[:-1]) < 0.1 and cos_time[-1] != '秒':
            # if float(res_time[:-1]) < 0.1 and cos_time[-1] == "天":
            try:
                conn = http.client.HTTPConnection(ip, port, timeout=3.0)
                conn.request(method='GET', url=myurl, headers=requestHeader)
                res = conn.getresponse()
                ip_list.append(ip + ":" + port)
                if(len(ip_list) > 3):
                    break
            except:
                pass

    tmp.close()
    return ip_list

# 获取代理IP
def get_random_ip():
    proxy_list = []
    for ip in IPlIST:
    # for ip in getiplist():
        proxy_list.append('http://' + ip)
    proxy_ip = random.choice(proxy_list)
    proxies = {'http': proxy_ip}
    return proxies


# 获取城市及对应地区码
def getCitys():
	try:
		City_lists = {}
		# 访问网址
		url = 'http://zfcxjst.hlj.gov.cn:6066/cljg/index.aspx'
		# 这是代理IP
		proxy = get_random_ip()

		# 创建ProxyHandler
		proxy_support = request.ProxyHandler(proxy)

		# 创建Opener
		opener = request.build_opener(proxy_support)
		# 添加User Angent
		opener.addheaders = [{'User-Agent', ua.Chrome}]
		# 安装OPener
		request.install_opener(opener)
		# 使用自己安装好的Opener
		response = request.urlopen(url, timeout=5)

		with response:
			text = response.read()
			text = text.decode('utf-8')
			html = etree.HTML(text)
			# list_a=html.xpath('//dl[@class="tbox"]//li/a')
			# for index in range(len(list_a)):
			#     if(index%2==0):
			# print(list_a[index].tag)
			# print(list_a[index].attrib)
			# print(list_a[index].text)
			list_a = html.xpath('//dl[@class="tbox"]//li/a/@onclick')
			for citybm in list_a:
				city = html.xpath('//dl[@class="tbox"]//li/a[@onclick="%s"]/text()' % citybm)
				City_lists[city[0]] = citybm.split("'")[1]
				# print(City_lists[city[0]])

		# print(type(City_lists))
		insertcity(City_lists)
		return City_lists

	except:
		# print("f")
		getCitys()

#插入材料
def insertCity(City_lists):
	# 打开数据库连接
	db = MySQLdb.connect("localhost","test","123456","hljxxj",charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# 使用execute方法执行SQL语句
	cursor.execute("SELECT VERSION()")

	# 使用 fetchone() 方法获取一条数据
	data = cursor.fetchone()

	try:
   		for cl in City_lists.keys:
   			isexist='''select 1 from city_reg where regionid="%s"'''%City_lists[cl]
   			cursor.execute(isexist)
   			if cursor.fetchone() is None:
   				pass
   			else:
   				continue

   			sql="""INSERT INTO city_reg(city,
         	regionid, parent_reg)
         	VALUES ('%s', '%s', '%s')"""%(cl['fid'],cl['fname'],cl['ftime'],cl['model'],cl['mprice'],cl['mpricetax0'],cl['region'],cl['remarks'],cl['unit'])

   			# 执行sql语句
   			cursor.execute(sql)

   			# 执行sql语句
   			db.commit()

	except:
   		# Rollback in case there is any error
   		db.rollback()

	# 关闭数据库连接
	db.close()

#获取县级地区
def getSecond_City(regionid):
	try:
		City_lists ={}
		# 访问网址
		url = 'http://zfcxjst.hlj.gov.cn:6066/cljg/ajaxtools.aspx'
		# 这是代理IP
		proxy = get_random_ip()

		data = urlencode({
			'dopost': 'second_regionid',
			'regionid': '%s' % regionid,
			'rnd': ''
			})

		# 创建ProxyHandler
		proxy_support = request.ProxyHandler(proxy)

		# 创建Opener
		opener = request.build_opener(proxy_support)
		# 添加User Angent
		opener.addheaders = [{'User-Agent', ua.Chrome}]
		# 安装OPener
		request.install_opener(opener)
		# 使用自己安装好的Opener
		response = request.urlopen(url, timeout=5,data=data.encode())
		with response:
			text = response.read()
			text = text.decode('utf-8')
			html = etree.HTML(text)
			list_a = html.xpath('//li/a')
			for tmp in list_a:
				city = tmp.xpath('text()')
				if(city is None):
					continue
				city_regoind=re.findall(r"'(.+?)'",str(tmp.xpath('@onclick')))
				City_lists[city[0]] = city_regoind[0]

		insertcity(City_lists,regionid)
		return City_lists

	except:
		# print("error")
		getSecond_City(regionid)

# 标签
def getTags(regionid):
	try:
		list_data = {}
		# 访问网址
		url = 'http://zfcxjst.hlj.gov.cn:6066/cljg/ajaxtools.aspx'
		# 这是代理IP
		proxy = get_random_ip()

		data = urlencode({
			'dopost': 'regionid_price',
			'regionid': '%s' % regionid,
			'ryear': 'null',
			'rmonth': 'null',
			'keyword': ' ',
			'rnd': ''
			})

		# 创建ProxyHandler
		proxy_support = request.ProxyHandler(proxy)

		# 创建Opener
		opener = request.build_opener(proxy_support)
		# 添加User Angent
		opener.addheaders = [{'User-Agent', ua.ie}]
		# 安装OPener
		request.install_opener(opener)
		# 使用自己安装好的Opener
		response = request.urlopen(url, timeout=5,data=data.encode())

		with response:
			text = response.read()
			text = text.decode('utf-8')
			html = etree.HTML(text)
			list_tags = html.xpath('//a/@onclick')
			list_tag = html.xpath('//a/text()')
			for index in range(len(list_tag)):
				tmp = re.findall(r"'(.+?)'", list_tags[index])
				mid = tmp[2]
				regionid = tmp[3]
				list_data[list_tag[index]] = tag(mid, regionid,list_tag[index])
		if(len(list_data)>0):
			inserttag(list_data)
			print("inserttag")
		return list_data

	except:
		getTags(regionid)

# 时间
def getTime(regionid):
	try:
		list_data = {}
		# 访问网址
		url = 'http://zfcxjst.hlj.gov.cn:6066/cljg/ajaxtools.aspx'
		# 这是代理IP
		proxy = get_random_ip()
		data = urlencode({
			'dopost': 'regionid_time',
			'regionid': '%s' % regionid,
			'rnd': ''
			})

		# 创建ProxyHandler
		proxy_support = request.ProxyHandler(proxy)

		# 创建Opener
		opener = request.build_opener(proxy_support)
		# 添加User Angent
		opener.addheaders = [{'User-Agent', ua.Chrome}]
		# 安装OPener
		request.install_opener(opener)
		# 使用自己安装好的Opener
		response = request.urlopen(url, timeout=5,data=data.encode())
		with response:
			text = response.read()
			text = text.decode('utf-8')
			html = etree.HTML(text)
			list_years = html.xpath('//select[@id="yera_select"]/option/text()')
			list_months = html.xpath('//select[@id="month_select"]/option/text()')
			for index in range(len(list_years)):
				if(index == 0):
					year = list_years[index]
					months = list_months
					list_data[year] = months
				else:
					year = list_years[index]
					list_data[year] = ['1月', '2月', '3月', '4月', '5月','6月', '7月', '8月', '9月', '10月', '11月', '12月']

		# if(list_data==None):
		# 	print("None")
		# tmp=len(list_data)
		return list_data

	except:
		getTime(regionid)


# 材料数据
def getCl(mid, regionid, ryear, rmonth):
	try:
		list_data = {}
		# 访问网址
		url = 'http://zfcxjst.hlj.gov.cn:6066/cljg/ajaxtools.aspx'
		# 这是代理IP
		proxy = get_random_ip()

		data = urlencode({'dopost': 'materialList', 'pagesize': '50',
			'mid': '%s' % mid,  # BE14195D-9441-45ED-A62E-8977E5BD58EC
			'regionid': '%s' % regionid,
			'keyword': '',
			'pageno': '1',
			'ryear': '%s' % ryear,
			'rmonth': '%s' % rmonth,
			'keyword': '',
			'rnd': '',
			})

		# 创建ProxyHandler
		proxy_support = request.ProxyHandler(proxy)

		# 创建Opener
		opener = request.build_opener(proxy_support)
		# 添加User Angent
		opener.addheaders = [{'User-Agent', ua.Chrome}]
		# 安装OPener
		request.install_opener(opener)
		# 使用自己安装好的Opener
		response = request.urlopen(url, timeout=5,data=data.encode())
		with response:
			text = response.read()
			text = text.decode('utf-8')
			# print(text)
			d = json.loads(text)
			pagesize = d['listpage']['pagesize']
			pageno = d['listpage']['pageno']
			recordcount = d['listpage']['recordcount']
			pagecount = d['listpage']['pagecount']
			# list_data = d['listpage']['listdata']
			if(int(recordcount)==0 and int(pagecount)==0):
				return True
			getCl_pagesize(mid,regionid,ryear,rmonth,recordcount)

		# insertCl(list_data)

		return True

		# return list_data

	except:
		# print('2')
		getCl(mid, regionid, ryear, rmonth)

def getCl_pagesize(mid, regionid, ryear, rmonth,pagesize):
	try:
		list_data = {}
		# 访问网址
		url = 'http://zfcxjst.hlj.gov.cn:6066/cljg/ajaxtools.aspx'
		# 这是代理IP
		proxy = get_random_ip()

		data = urlencode({'dopost': 'materialList', 'pagesize': '%s'%pagesize,
			'mid': '%s' % mid,  # BE14195D-9441-45ED-A62E-8977E5BD58EC
			'regionid': '%s' % regionid,
			'keyword': '',
			'pageno': '1',
			'ryear': '%s' % ryear,
			'rmonth': '%s' % rmonth,
			'keyword': '',
			'rnd': '',
			})

		# 创建ProxyHandler
		proxy_support = request.ProxyHandler(proxy)

		# 创建Opener
		opener = request.build_opener(proxy_support)
		# 添加User Angent
		opener.addheaders = [{'User-Agent', ua.Chrome}]
		# 安装OPener
		request.install_opener(opener)
		# 使用自己安装好的Opener
		response = request.urlopen(url, timeout=5,data=data.encode())
		with response:
			text = response.read()
			text = text.decode('utf-8')
			# print(text)
			d = json.loads(text)
			pagecount = d['listpage']['pagecount']
			list_data = d['listpage']['listdata']
		insertCl(mid,list_data)

		return True

		# return list_data

	except:
		# print('2')
		getCl(mid, regionid, ryear, rmonth)


#插入材料
def insertCl(mid,list_data):
	# 打开数据库连接
	db = MySQLdb.connect("localhost","test","123456","hljxxj",charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# # 使用execute方法执行SQL语句
	# cursor.execute("SELECT VERSION()")

	# # 使用 fetchone() 方法获取一条数据
	# data = cursor.fetchone()
	# print(list_data)
	try:
   		for cl in list_data:
   			isexist = '''select 1 from cl where fid="%s" and ftime="%s"''' % (cl['fid'], cl['ftime'])
   			d=cursor.execute(isexist)
   			
   			if cursor.fetchone() is None:
   				pass
   			else:
   				continue

   			sql="""INSERT INTO cl(fid,
         	fname, ftime, model, mprice,mpricetax0,region,remarks,unit,tagfid)
         	VALUES ('%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s')"""%(cl['fid'],cl['fname'],cl['ftime'],cl['model'],cl['mprice'],cl['mpricetax0'],cl['region'],cl['remarks'],cl['unit'],mid)

   			# 执行sql语句
   			cursor.execute(sql)

   			# 执行sql语句
   			db.commit()

	except:
   		# Rollback in case there is any error
   		db.rollback()
   		print("f")

	# 关闭数据库连接
	db.close()

#插入标签
def inserttag(tag_list):
	# 打开数据库连接
	db = MySQLdb.connect("localhost","test","123456","hljxxj",charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	# # 使用execute方法执行SQL语句
	# cursor.execute("SELECT VERSION()")

	# # 使用 fetchone() 方法获取一条数据
	# data = cursor.fetchone()
	# print(list_data)
	try:
   		for key_tag in tag_list:
   			tags_mid=tag_list[key_tag].mid
   			tags_regoinid=tag_list[key_tag].regionid
   			tags_name=tag_list[key_tag].tag
   			isexist = '''select 1 from tags where tagfid="%s" and regionid="%s"''' % (tags_mid,tags_regoinid)
   			d=cursor.execute(isexist)
   			
   			if cursor.fetchone() is None:
   				pass
   			else:
   				continue

   			sql="""INSERT INTO tags(tagfid,
         	regionid, tag_name)
         	VALUES ('%s', '%s', '%s')"""%(tags_mid,tags_regoinid,tags_name)

   			# 执行sql语句
   			cursor.execute(sql)

   			# 执行sql语句
   			db.commit()

	except:
   		# Rollback in case there is any error
   		db.rollback()
   		print("f")

	# 关闭数据库连接
	db.close()

#插入城市
def insertcity(city_list,parent_regionid=""):
	# 打开数据库连接
	db = MySQLdb.connect("localhost","test","123456","hljxxj",charset='utf8')
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()

	try:
   		for city in city_list.keys():
   			city_regionid=city_list[city]
   			isexist = '''select 1 from city where regionid="%s"''' % (city_regionid)
   			d=cursor.execute(isexist)
   			
   			if cursor.fetchone() is None:
   				pass
   			else:
   				continue
   			sql='''INSERT INTO city(name, regionid,parent_regionid)
         	VALUES ("%s", "%s", "%s")'''%(city,city_regionid,parent_regionid)

   			# 执行sql语句
   			cursor.execute(sql)

   			# 执行sql语句
   			db.commit()

   			# for city in city_list.keys():
    		# print(city)
    		# city_regionid=city_list[city]   #市级地区
    		# city_second_list={}
    		# city_second_list=getSecond_City(city_regionid)

	except:
   		# Rollback in case there is any error
   		db.rollback()
   		print("f")

	# 关闭数据库连接
	db.close()

class tag:

    def __init__(self, mid, regionid,tag):
        self.mid = mid
        self.regionid = regionid
        self.tag=tag

def cl_spider():
    try_times = 0
    tm = 1   
    try:
    	city_list={'哈尔滨市': '230100', '齐齐哈尔市': '230200', '牡丹江市': '231000', '佳木斯市': '230800', '大庆市': '230600', '鸡西市': '230300', '双鸭山市': '230500', '伊春市': '230700', '七台河市': '230900', '鹤岗市': '230400', '绥化市': '231200', '黑河市': '231100', '大兴安岭地区': '232700'}
    	# city_list=getCitys()
    	while city_list is None:
    		city_list=getCitys()
    	print(type(city_list))
    	print("city")
    	city_no=["哈尔滨市"]
    	for city in city_list.keys():
    		if(city not in city_no):
    			continue
    		print(city)
    		city_regionid=city_list[city]   #市级地区
    		city_second_list={}
    		city_second_list=getSecond_City(city_regionid)
    		while(city_second_list is None):
    			city_second_list=getSecond_City(city_regionid)
    		print(type(city_second_list))
    		print(city_second_list)
    		tags_list={}
    		time_list={}
    		tag_list={}
    		cl_list={}
    		# if city_second_list is None or len(city_second_list)==0:
    		if True:
    			# print("No_second")
    			time_list=getTime(city_regionid)
    			while time_list is None:
    				time_list=getTime(city_regionid)

    			if time_list is None or len(time_list)==0:
    				continue
    			for key_time in time_list.keys():
    				ryear=key_time
    				if("2020" not in ryear):
    					continue
    				rmonth=time_list[key_time]
    				print(ryear)
    				print(rmonth)
    				tag_list=getTags(city_regionid)
    				while tag_list is None:
    					tag_list=getTags(city_regionid)

    				for key_tag in tag_list:
    					print(key_tag)
    					tags_mid=tag_list[key_tag].mid
    					tags_regoinid=tag_list[key_tag].regionid
    					print("cl")
    					for month in rmonth:
    						issuccess=getCl(tags_mid,tags_regoinid,ryear[:-1],month[:-1])
    						if issuccess==True:
    							print("Success")
    					
    		if len(city_second_list)>0:
    			print("second")
    			for city_second in city_second_list:
    				print(city_second)
    				city_regionid=city_second_list[city_second]
    				time_list=getTime(city_regionid)
    				while time_list is None:
    					time_list=getTime(city_regionid)
    				print("time")
    				print(time_list)
    				if time_list is None or len(time_list)==0:
    					continue
    				for key_time in time_list.keys():
    					ryear=key_time
    					if("2020" in ryear):
    						continue
    					rmonth=time_list[key_time]
    					print(ryear)
    					print(rmonth)
    					tag_list=getTags(city_regionid)
    					while tag_list is None:
    						tag_list=getTags(city_regionid)
    					print(type(tag_list))
    					print("tag")
    					for key_tag in tag_list:
    						print(key_tag)
    						tags_mid=tag_list[key_tag].mid
    						tags_regoinid=tag_list[key_tag].regionid
    						print("cl")
    						for month in rmonth:
    							issuccess=getCl(tags_mid,tags_regoinid,ryear[:-1],month[:-1])
    							if issuccess==True:
    								print("Success")

    except HTTPError as e:
    	print(e)



cl_spider()
# getCl('BE14195D-9441-45ED-A62E-8977E5BD58EC','230100','2020','4')
# insertCl(cl)
# getCl('B80FC028-B390-4E44-84AF-81F3EDD1ED35','230100','2020','5')
# getTags("230100")
# print(getSecond_City("230200"))
# print(getTime("230201"))

# city_list={'哈尔滨市': '230100', '齐齐哈尔市': '230200', '牡丹江市': '231000', '佳木斯市': '230800', '大庆市': '230600', '鸡西市': '230300', '双鸭山市': '230500', '伊春市': '230700', '七台河市': '230900', '鹤岗市': '230400', '绥化市': '231200', '黑河市': '231100', '大兴安岭地区': '232700'}

# insertcity(city_list)



