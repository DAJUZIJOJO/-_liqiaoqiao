#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('UTF-8')
import subprocess
import ConfigParser
import requests
import time
import datetime
import json
import copy
import re
import math
import os
import gzip
import StringIO
import time
from functools import partial
from pyspark.sql import HiveContext,SQLContext,Row
from pyspark.sql.types import StructField, StringType, FloatType, StructType
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark import SparkFiles

######################################初始化参数##########################################
#判断日期是否合法
def judge_date_valid(date_str):
    try:
        datetime.datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False

#define global parameter
CTRL_A = "\x01"
CTRL_B = "\x02"
#data parameter
deal_date = time.strftime('%Y%m%d',time.localtime(time.time()))
deal_section = ''
#abs_path = '/'.join(sys.argv[0].split('/')[0:len(sys.argv[0].split('/'))-1])
abs_path = os.path.dirname(os.path.abspath(__file__))
#print "abs_path",os.path.dirname(os.path.abspath(__file__))
if len(sys.argv)<3:
    print 'The count of parameter is wrong!'
    print 'Please input like: python file_parse.py yyyymmdd conf_file.'
    sys.exit()
else:
    deal_date = sys.argv[1]
    deal_section = sys.argv[2].strip()
    abs_path = os.path.dirname(os.path.abspath(__file__))
    if not judge_date_valid(deal_date):
        print 'Please input like: python file_parse.py yyyymmdd conf_file.'
        sys.exit()
date_p = datetime.datetime.strptime(deal_date,'%Y%m%d').date()
yestoday = date_p + datetime.timedelta(days=-1)
last_deal_date = datetime.datetime.strftime(yestoday,'%Y%m%d')
deal_year = deal_date[:4]
deal_month = deal_date[4:6]
deal_day = deal_date[-2::]

print '*'*100
print 'Run date:',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
print 'Deal date:',deal_date

#重命名包的后缀
pwd=os.getcwd()
print os.getcwd()
print [d for d in os.listdir('.')]
os.rename('xlrd.conf','xlrd.zip')
print [d for d in os.listdir('.')]
# init pyspark
# ${SPARK_HOME}/python/pyspark/shell.py
# SPARK_HOME=/usr/local/spark-current
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
#spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.addPyFile("xlrd.zip")
hiveCtx = HiveContext(sc)
sqlCtx = SQLContext(sc)

import xlrd
from xlrd import xldate_as_tuple

# get config
cf = ConfigParser.ConfigParser()
cf.read(('{abs_path}/{deal_section}.conf'.format(abs_path=abs_path,deal_section=deal_section)).replace('.conf.conf','.conf'))
#print ('{abs_path}/{deal_section}.conf'.format(abs_path=abs_path,deal_section=deal_section)).replace('.conf.conf','.conf')
#print 'abs_path:',abs_path
#print 'sys.argv',sys.argv
#print cf.sections()

# output log
def print_log(msg):
    print time.strftime('%Y-%m-%d %H:%M:%S ::: ',time.localtime(time.time())) + str(msg)

# read config
def read_config(section,para):
    ret_str = ''
    try:
        ret_str = cf.get(section,para)
    except Exception, e:
        #print 'Zhere is no',para,'in',section
        print_log('There is no' + para + 'in' + section)
    return ret_str

# read value from config
def read_config_dict():
    conf_dict = {}
    #获取参数组成字典
    for section in cf.sections():
        temp_dict = {}
        for option in cf.options(section):
            opt_value = read_config(section,option)
            temp_dict[option] = opt_value
        conf_dict[section] = temp_dict
    if conf_dict:
        conf_dict['conf_file'] = deal_section
        return conf_dict
    else:
        return '{abs_path}/{deal_section}.conf'.format(abs_path=abs_path,deal_section=deal_section)

#get data from Cooper
def get_data_from_cooper(user_name="hanjianlong",resource_id="2199049624304",api_key="3fc857863ded4690afe9fd33cd7d95f6",group_id="114922"):
	global CTRL_A
	global CTRL_B
	#cooper API
	req_files_list_url = "http://api-kylin.intra.xiaojukeji.com/EP_CLOUD_DISK_oe_openapi_cooper_server_prod/openapi/v1/files"
	req_files_preview_url = "http://api-kylin.intra.xiaojukeji.com/EP_CLOUD_DISK_oe_openapi_cooper_server_prod/openapi/v1/files/preview"
	req_files_download_url = "http://api-kylin.intra.xiaojukeji.com/EP_CLOUD_DISK_oe_openapi_cooper_server_prod/openapi/v1/files/downloads"
	req_files_export_url = "http://api-kylin.intra.xiaojukeji.com/EP_CLOUD_DISK_oe_openapi_cooper_server_prod/openapi/v1/cooperate/export/{resourceId}"
	#get current path
	cur_path=os.getcwd()
	#define file name
	file_name="{curPath}/{resourceId}.xlsx"
	output_file="{curPath}/{resourceId}.txt"
	#request header
	header = {
		"Apikey": api_key,
		"username": user_name
	}
	#request parameter
	payload = {
		"group_id":group_id
	}
	#request body
	data = {
		"ids":[2199049624304,2199043683070]
	}
	#download data from cooper
	#r = requests.post(url=req_files_download_url, headers=header, params=payload, data=json.dumps(data))
	#print r.text
	#get the file list
	#r = requests.get(url=req_files_list_url, headers=header, params=payload)
	#print r.text
	#get file stream
	stream_read = requests.get(url=req_files_export_url.format(resourceId=resource_id), headers=header, params=payload, stream=True)
	#generate excel
	with open(file_name.format(curPath=cur_path,resourceId=resource_id),"wb") as xx:
		xx.write(stream_read.content)
	#read excel
	data = xlrd.open_workbook(file_name.format(curPath=cur_path,resourceId=resource_id))
	print(type(data))
	table = data.sheets()[0]
	nrows = table.nrows
	ncols = table.ncols
	#get excel title
	title_list = []
	title_list = [str(table.cell_value(0,iCol)) for iCol in range(ncols)]
	#generate txt
	f = open(output_file.format(curPath=cur_path,resourceId=resource_id),"wb")
	for iRow in range(1,nrows):
		row = []
		for iCol in range(ncols):
			sCell = table.cell_value(iRow,iCol)
			ctype = table.cell(iRow,iCol).ctype
			if ctype == 3:
				try:
					year, month, day, hour, minute, second = xldate_as_tuple(sCell, 0)
					if sCell < 1:
						sCell = "{hour}:{minute}:{second}".format(hour=hour,minute=minute,second=second)
					else:
						sCell = "{year}-{month}-{day} {hour}:{minute}:{second}".format(year=year,month=month,day=day,hour=hour,minute=minute,second=second)
				except:
					sCell = str(sCell)
			elif ctype == 2 and sCell % 1 == 0:
				sCell = str(int(sCell))
			elif ctype == 4:
				sCell = True if sCell == 1 else False
			else:
				sCell = str(sCell)
			row.append(sCell.replace("\n"," ").replace("\r"," "))
		if (nrows-1) == iRow:
			f.write(CTRL_A.join(row))
		else:
			f.write(CTRL_A.join(row) + "\n")
	f.close()
	return (title_list,output_file.format(curPath=cur_path,resourceId=resource_id))

#load data into hive
def load_data_into_hive(db_name="",tab_name="",file_path="",title_list=[]):
	global deal_date
	global deal_year
	global deal_month
	global deal_day
	global CTRL_A
	global CTRL_B
	#define row
	def_col = """ {column} string comment '{comment}' """
	def_cols = ""
	columns = ""
	for idx,title in enumerate(title_list):
		col_name = "col_" + str(idx)
		comment = title
		if not def_cols:
			columns = col_name
			def_cols = def_col.format(column=col_name,comment=comment)
		else:
			columns = columns + ',' + col_name
			def_cols = def_cols + ',' + def_col.format(column=col_name,comment=comment)
	create_table_sql = """create table if not exists {db_name}.{tab_name}
						(
							{def_cols}
						)
						PARTITIONED BY (year string,month string,day string)
						row format delimited
						fields terminated by '{row_t}'
						lines terminated by '\n'
						stored as textfile
					""".format(db_name=db_name,tab_name=tab_name,def_cols=def_cols,line_n=CTRL_B,row_t=CTRL_A)
	#create table 
	print_log(create_table_sql)
	hiveCtx.sql(create_table_sql)
	load_file_into_hdfs = """
					load data local inpath '{file_path}'
					overwrite into table {db_name}.{tab_name}
					PARTITION(year = '{year}',month='{month}',day='{day}')
					"""
	print_log(load_file_into_hdfs.format(db_name=db_name,tab_name=tab_name,file_path=file_path,year=deal_year,month=deal_month,day=deal_day))
	hiveCtx.sql(load_file_into_hdfs.format(db_name=db_name,tab_name=tab_name,file_path=file_path,year=deal_year,month=deal_month,day=deal_day))

######################################程序入口判断部分##########################################
#get value from config
loop_dict = read_config_dict() 

# regenerate dict from config
def rebuild_para(section_list):
    global loop_dict
    ret_dict = {}
    for section in section_list:
        #print section
        #print loop_dict[section]
        for key in loop_dict[section].keys():
            ret_dict[key] = loop_dict[section][key]
    return ret_dict

# main function
def main():
    #define date
    file_date=deal_date
    #define parameter
    global loop_dict
    if not isinstance(loop_dict,dict):
        #print 'There is no data in configuration file.'
        #print 'The path of configuration file, please check:',loop_dict
        print_log('There is no data in configuration file.')
        print_log(loop_dict)
        sys.exit()
    else:
        #print 'loop_dict:',loop_dict
        print_log('loop_dict:' + str(loop_dict))
    #get common config
    common_dict = loop_dict['common']
    #database parameter
    db_name = common_dict["db_name"]
    tab_name = common_dict["tab_name"]
    api_key = common_dict["api_key"]
    group_id = common_dict["group_id"]
    user_name = common_dict["user_name"]
    resource_id = common_dict["resource_id"]
    print common_dict
    (title_list,output_file) = get_data_from_cooper(user_name=user_name,resource_id=resource_id,api_key=api_key,group_id=group_id)
    print title_list
    print [d for d in os.listdir('.')]
    if output_file:
    	load_data_into_hive(db_name=db_name,tab_name=tab_name,file_path=output_file,title_list=title_list)

#program entrance
#if __name__ == "__main__":
#test_main()
main()