#!/usr/bin/env python
# encoding: utf-8
"""
@version: v1.0
@author: W_H_J
@license: Apache Licence
@contact: 415900617@qq.com
@site:
@software: PyCharm
@file: dparkTest.py
@time: 2018/6/5 16:38
@describe: PI值估算
"""
from random import random
import dpark

# 定义取点总数，越多越精确
N = 100000


# 定义map()转换函数，用于将RDD转变成新的RDD
def rand(i):
    x = random()
    y = random()
    return (x * x + y * y) < 1.0 and 1 or 0


# (1)dpark.parallelize将数据分成4块RDD
# (2)利用map()将4块RDD对应转换成新的RDD
# (3)利用reduce()处理每一块RDD，返回计算结果
count = dpark.parallelize(range(N), 4).map(rand).reduce(lambda x, y: x + y)

print ('pi is ', 4.0 * count / N)


