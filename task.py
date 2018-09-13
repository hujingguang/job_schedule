#!/usr/bin/python
# -*- coding=UTF-8 -*-

import random
import logging
import os
import time
import json
from logging.handlers import RotatingFileHandler
import sys
import threading
import yaml
import requests
from datetime import datetime
'''
该脚本用来做任务调度
请先修改配置文件再运行 默认配置文件./task.yml

task.yml格式 如下
-----------------------------------------------

job1:
  - /usr/local/bin/job1.sh
  - /usr/local/bin/job2.sh
  - required:
     - job2
job2:
  - /usr/local/bin/job3.sh
  - /usr/local/bin/job4.sh

-----------------------------------------------

job1在job2执行完脚本 job3.sh和job4.sh之后再执行,job3,job4并行执行
'''
__author__='Hoover'
LOG_INFO={
	'log_level':logging.INFO,
	'log_format':'%(asctime)-15s %(levelname)s %(lineno)s %(message)s ',
	'log_file':'/tmp/task.log',
	'log_max_size':1000000000,
	'log_backup':15,
	}
DEBUG=False
DEFAULT_CONF_FILE=os.path.join(os.path.abspath(os.path.dirname(__file__)),'task.yml')
def init_logger():
    global DEBUG,LOG_INFO
    logging.basicConfig(level=logging.DEBUG,filename='/dev/null')
    LOGGER=logging.getLogger('Task')
    log_format=logging.Formatter(LOG_INFO.get('log_format',''))
    log_file=LOG_INFO.get('log_file')
    log_level=LOG_INFO.get('log_level')
    if DEBUG:
	stream_handler=logging.StreamHandler()
	stream_handler.setLevel(log_level)
	stream_handler.setFormatter(log_format)
	LOGGER.addHandler(stream_handler)
    else:
	log_folder=os.path.dirname(LOG_INFO.get('log_file'))
	if not os.path.exists(log_folder):
	    os.makedirs(log_folder)
	log_max_size=LOG_INFO.get('log_max_size')
	log_backup=LOG_INFO.get('log_backup')
	rotate_handler=RotatingFileHandler(log_file,'a',log_max_size,log_backup)
	rotate_handler.setFormatter(log_format)
	rotate_handler.setLevel(log_level)
	LOGGER.addHandler(rotate_handler)
init_logger()
logger=logging.getLogger('Task')
_STATUS={}
_DEPENDENCY={}
_REPORT_TASK="任务执行情况报告: \n"
_ERROR_TASK=''
_OK_TASK=''

def load_configure(conf_file=DEFAULT_CONF_FILE):
    assert os.path.exists(conf_file),"文件不存在！"
    conf_dict={}
    syntax_error='语法错误'
    syntax_sub1_error='不能依赖自己!'
    syntax_sub2_error='不能互相依赖!'
    def _check_conf_is_ok(f):
	assert isinstance(f,file)
	try:
	    f=yaml.load(f)
	except Exception as e:
	    logger.info(str(e))
	    sys.exit(1)
	assert isinstance(f,dict),syntax_error
	for k,v in f.iteritems():
	    assert isinstance(v,list),syntax_error
	    script_list=[]
	    job_list=[]
	    for key in v:
		if isinstance(key,dict):
		    if len(key)>1 or 'required' not in key:
			assert False,syntax_error
		    else:
			jobs=key['required']
			assert isinstance(jobs,list),syntax_error
			for job in jobs:
			    assert isinstance(job,str),syntax_error
			    job_list.append(job)
		elif isinstance(key,str):
		    script_list.append(key)
	    conf_dict[k]={'scripts':script_list,'jobs':job_list}
	for job_name in conf_dict.iterkeys():
	    jobs=conf_dict[job_name]['jobs']
	    assert not job_name in jobs,syntax_error+syntax_sub1_error+" Job: "+job_name
	    for job in jobs:
		try:
		    other_jobs=conf_dict[job]['jobs']
		except KeyError:
		    logger.info('不存在Job: %s' %str(job))
		    sys.exit(1)
		else:
		    assert not job_name in other_jobs,syntax_error+syntax_sub2_error+" Job: "+job_name+','+job
    with open(conf_file) as f:
	_check_conf_is_ok(f)
    return conf_dict

def init_task(conf):
    global _STATUS
    for job_name in conf.iterkeys():
	_STATUS[job_name]={}
	_DEPENDENCY[job_name]=[]
	for script in conf[job_name]['scripts']:
	    _STATUS[job_name][script]=0
	for job in conf[job_name]['jobs']:
	    _DEPENDENCY[job_name].append(job)


def _check_dependency_job(job):
    global _STATUS,_DEPENDENCY
    depend_jobs=_DEPENDENCY[job]
    FLAG=1
    Failed=0
    Running=0
    for depend_job in depend_jobs:
	script_status=_STATUS[depend_job]
	for v in script_status.itervalues():
	    if v == 0:
		Running=1
	    if v == 2:
		Failed=1
    if Failed==1:
	return 2
    if Running==1:
	return 0
    return 1





def do_task(job,script):
    global _STATUS,_REPORT_TASK,_ERROR_TASK,_OK_TASK
    while True:
	result=_check_dependency_job(job)
	if result==1:
	    logger.info('开始执行Job: %s , Script: %s' %(job,script))
	    cmd='/bin/bash '+script+' &>/dev/null'
	    logger.info('do scripts: '+cmd)
	    try:
		begin_time=time.time()
		result=os.system(cmd)
		end_time=time.time()
		use_time=int(end_time-begin_time)
		if result != 0:
		   _STATUS[job][script]=2
		   _ERROR_TASK=_ERROR_TASK+'Job: '+job+" Script: "+script+'执行失败\n'
	        else:
		   _STATUS[job][script]=1
		   _OK_TASK=_OK_TASK+'Job: '+job+" Script: "+script+"执行成功  耗时: "+str(use_time)+'s\n'
		time.sleep(10)
	    except Exception as e:
		logger.error(str(e))
	    finally:
		break
	elif result == 0:
	    logger.info('Sleep 3s ,等待依赖任务执行完')
	    time.sleep(3)
	else:
	    logger.info('依赖任务执行失败,Job: %s 中断执行' %job)
	    _ERROR_TASK=_ERROR_TASK+'依赖任务执行失败 导致Job: '+job+'执行失败\n'
	    scripts_dict=_STATUS[job]
	    for k in scripts_dict.iterkeys():
		scripts_dict[k]=2
	    break


def main():
    global _STATUS
    threads=[]
    for job_name in _STATUS.iterkeys():
	scripts=_STATUS[job_name]
	for script in scripts.iterkeys():
	    threads.append(threading.Thread(target=do_task,args=(job_name,script)))
    for thread in threads:
	thread.start()
    for thread in threads:
	thread.join(timeout=600)
    logger.info(_STATUS)
    _REPORT_TASK=_ERROR_TASK+_OK_TASK
    send_message(_REPORT_TASK)




def send_message(mess):
    web_url='your dingding webhook address'
    mess=mess
    data={"msgtype":"text","text":{"content":mess}}
    headers={"Content-Type":"application/json"}
    try:
	r=requests.post(web_url,headers=headers,data=json.dumps(data))
    except Exception as e:
	logger.info(str(e))


if __name__=='__main__':
    if len(sys.argv) != 2:
	print 'Usage: ./task.py start|check  运行前请先运行 ./task.py check 检查配置文件是否正确! \n\n日志文件: /tmp/task.log 配置文件: '+ DEFAULT_CONF_FILE
	sys.exit(1)
    if sys.argv[1]== 'check':
	result=load_configure()
	print 'check ok '
    elif sys.argv[1] == 'start':
	result=load_configure()
	init_task(result)
	main()
    else:
	print 'Usage: ./task.py start|check'





