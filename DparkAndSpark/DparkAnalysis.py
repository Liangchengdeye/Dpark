#!/usr/bin/env python  
# encoding: utf-8  
""" 
@version: v1.0 
@author: W_H_J 
@license: Apache Licence  
@contact: 415900617@qq.com 
@site:  
@software: PyCharm 
@file: DparkAnalysis.py 
@time: 2018/6/7 11:40 
@describe: 初始化RDD并进行相应操作
进行每行记录解析
"""
from dpark import DparkContext
import os
# 导入格式化处理
from DparkAndSpark.Regularization import parseApacheLogLine
# 读取日志文件
baseDir = os.path.join('./data/NASA_LOG_MIN.txt')
# 初始化dpark
dc = DparkContext()


# 解析日志
def parseLogs():
    """ 读取和解析日志文件 """
    # 解析日志
    parsed_logs = (dc.textFile(baseDir)
                   .map(parseApacheLogLine)
                   .cache())
    # 解析成功
    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())
    # 解析失败
    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))

    # 失败条数统计
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print ('Number of invalid logline: %d' % failed_logs.count())
        for line in failed_logs.take(20):
            print ('Invalid logline: %s' % line)

    print ('*** Read %d lines, successfully parsed %d lines, failed to parse %d lines ***' % (parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()