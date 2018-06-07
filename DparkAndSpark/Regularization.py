#!/usr/bin/env python  
# encoding: utf-8  
""" 
@version: v1.0 
@author: W_H_J 
@license: Apache Licence  
@contact: 415900617@qq.com 
@site:  
@software: PyCharm 
@file: Regularization.py 
@time: 2018/6/7 11:30 
@describe: 对目标文件规则化处理---解析访问请求信息
s = '127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839'
"""
# 需要使用spark的ROW方法对返回值处理
# 可参考http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Row
from pyspark.sql import Row
from psutil import long
import re
import datetime

# 定义日期格式
month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

# 进行正则分割--数据清洗
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S*)\s*(\S*)\s*" (\d{3}) (\S+)'


# 处理时间日期格式
def parse_apache_time(s):
    """ 将Apache时间格式转换为Python datetime对象arg游戏:
        s (str): Apache时间格式的日期和时间。
    Returns:
        datetime: datetime object 忽略时区
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


# 对日志进行解析
def parseApacheLogLine(logline):
    """ 解析Apache公共日志格式中的一行
    Args:
        logline (str): Apache通用日志格式中的一行文本
    Returns:
        tuple: 一个包含Apache访问日志和1部分的字典，或者原始无效的日志行和0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    # Row方法是spark中的，返回相当于数据库一行记录，参考自开头说明链接
    return (Row(
        host=match.group(1),
        client_identd=match.group(2),
        user_id=match.group(3),
        date_time=parse_apache_time(match.group(4)),
        method=match.group(5),
        endpoint=match.group(6),
        protocol=match.group(7),
        response_code=int(match.group(8)),
        content_size=size
    ), 1)


