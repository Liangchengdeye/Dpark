#!/usr/bin/env python  
# encoding: utf-8  
""" 
@version: v1.0 
@author: W_H_J 
@license: Apache Licence  
@contact: 415900617@qq.com 
@site:  
@software: PyCharm 
@file: StatisticAnalysis404.py 
@time: 2018/6/7 17:03 
@describe: 探索404响应代码
参考自：https://blog.csdn.net/myjiayan/article/details/52463053?hmsr=toutiao.io&utm_medium=toutiao.io&utm_source=toutiao.io
"""
from DparkAndSpark.DparkAnalysis import access_logs
import matplotlib.pyplot as plt


# 404错误解析
def find404Error():
    # 查找404记录条数
    badRecords = access_logs.filter(lambda log:log.response_code == 404).cache()
    print('1=====>Found %d 404 URLs' % badRecords.count())
    # 列出404访问记录
    badEndpoints = badRecords.map(lambda log:(log.endpoint,1))
    badUniqueEndpoints = badEndpoints.reduceByKey(lambda a,b:a+b).map(lambda item:item[0])
    badUniqueEndpointsPick40 = badUniqueEndpoints.take(40)
    print('2=====>404 URLS: %s' % badUniqueEndpointsPick40)
    # 列出top20 404错误端点
    badEndpointsCountPairTuple = badRecords.map(lambda log:(log.endpoint,1))
    badEndpointsSum = badEndpointsCountPairTuple.reduceByKey(lambda a,b:a+b)
    badEndpointsTop20 = badEndpointsSum.sort(lambda item:-1*item[1]).take(20)
    print('3=====>Top-20 404 URLs: %s' % badEndpointsTop20)
    # 列出top20 404错误主机
    errHostsCountPairTuple = badRecords.map(lambda log:(log.host,1))
    errHostsSum = errHostsCountPairTuple.reduceByKey(lambda a,b:a+b)
    errHostsTop20 = errHostsSum.sort(lambda item:-1*item[1]).take(20)
    print('4=====>Top-20 hosts that generated errors: %s' % errHostsTop20)
    # 列出404每天的响应代码
    errDateCountPairTuple = badRecords.map(lambda log:(log.date_time.day,1))
    errDateSum = errDateCountPairTuple.reduceByKey(lambda a,b:a+b)
    errDateSorted = errDateSum.sort(lambda item:item[0]).cache()
    errByDate = errDateSorted.collect()
    print('5=====>404 Errors by day: %s' % errByDate)
    # 列出404前5天
    topErrDate = errDateSorted.sort(lambda item:item[1]*-1).take(5)
    print('6=====>Top Five dates for 404 requests: %s' % topErrDate)
    # 每小时404错误
    hourCountPairTuple = badRecords.map(lambda log:(log.date_time.hour,1))
    hourRecordsSum = hourCountPairTuple.reduceByKey(lambda a,b:a+b)
    hourRecordsSorted = hourRecordsSum.sort(lambda item:item[0]).cache()
    errHourList = hourRecordsSorted.collect()
    print('7=====>Top hours for 404 requests: %s' % errHourList)
    hoursWithErrors404 = hourRecordsSorted.map(lambda item:item[0]).collect()
    errors404ByHours = hourRecordsSorted.map(lambda item:item[1]).collect()
    # 每小时404错误-可视化
    fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
    plt.axis([0, max(hoursWithErrors404), 0, max(errors404ByHours)])
    plt.grid(b=True, which='major', axis='y')
    plt.xlabel('Hour')
    plt.ylabel('404 Errors')
    plt.plot(hoursWithErrors404, errors404ByHours)
    plt.savefig("./img/404DayHour.png")


if __name__ == '__main__':
    find404Error()