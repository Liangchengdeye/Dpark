#!/usr/bin/env python  
# encoding: utf-8  
""" 
@version: v1.0 
@author: W_H_J 
@license: Apache Licence  
@contact: 415900617@qq.com 
@site:  
@software: PyCharm 
@file: wordcount.py 
@time: 2018/6/5 18:10 
@describe:  单词统计
"""
from dpark import DparkContext
ctx = DparkContext()
file = ctx.textFile("./words.txt")
words = file.flatMap(lambda x:x.split()).map(lambda x:(x,1))
wc = words.reduceByKey(lambda x,y:x+y).collectAsMap()
print (wc)


# 统计单词出现的个数
def word_count(file_path, word):
    # 指定某个Mesos主机进行沟通
    dpark = DparkContext()

    # 将分布式文件，构造成文件RDD,每块大小为16m
    f = dpark.textFile(file_path, splitSize=16 << 20)

    # 用map()转变成新的RDD,再用filter过滤出更新的RDD，最后用count()处理返回结果
    print(word, 'count:', f.map(lambda line: line.strip()).filter(lambda line: word in line).count())


word_count("./words.txt", "php")