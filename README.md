# Dpark
Dpark-AND-Spark<br>
Dpark：Dpark是国内豆瓣公司根据Spark进行的克隆版本的实现<br>
DPark 是一个类似MapReduce 的基于Mesos（Apache 下的一个集群管理器，提供了有效的、跨分布式应用或框架的资源隔离和共享等功能）的集群并行计算框架（Cluster Computing Framework），DPark 是Spark 的Python克隆版本，是一个Python 实现的分布式计算框架，可以非常方便地实现大规模数据处理和低延时的迭代计算。该计算框架类似于MapReduce，但是比其更灵活，可以用Python 非常方便地进行分布式计算，并且提供了更多的功能，以便更好地进行迭代式计算。DPark 由国内的豆瓣公司开发实现和负责维护，据豆瓣公司的描述，目前豆瓣公司内部的绝大多数数据分析都使用DPark 完成，整个项目也正趋于完善。<br>
Dpark克隆与Spark<br>
参考网站：http://suanfazu.com/t/dpark-de-chu-bu-shi-yong/444 <br>
本例实现参考自：https://blog.csdn.net/myjiayan/article/details/52463053?hmsr=toutiao.io&utm_medium=toutiao.io&utm_source=toutiao.io <br>
Spark官方文档：http://spark.apache.org/docs/latest/api/python/pyspark.sql.html <br>
Dpark：https://github.com/douban/dpark <br>
简介： <br>
DPark is a Python clone of Spark, MapReduce(R) alike computing framework supporting iterative computation. <br>
Example for word counting (wc.py): <br>
from dpark import DparkContext <br>
ctx = DparkContext() <br>
file = ctx.textFile("/tmp/words.txt") <br>
words = file.flatMap(lambda x:x.split()).map(lambda x:(x,1)) <br>
wc = words.reduceByKey(lambda x,y:x+y).collectAsMap() <br>
print wc <br>
This script can run locally or on a Mesos cluster without any modification, just using different command-line arguments: <br>
$ python wc.py <br>
$ python wc.py -m process <br>
$ python wc.py -m host[:port] <br>
参考资料：<br>
1:https://blog.csdn.net/ns2250225/article/details/43484369 <br>
2:dpark和spark区别https://blog.csdn.net/sanqima/article/details/51201067 <br>
3：官方资料https://github.com/jackfengji/test_pro/wiki <br>
<br>#---------------------------------------------------------------------------------------------------------------------------------------#
关于本例：./Dpark/Dpark_Test 目录为网上demo，一个是PI值估算，一个是wordcount； <br>
./Dpark/DparkAndSpark 目录主要解析网页访问请求'127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839'
解析不同状态等信息获取所需消息。 <br>
./data/NASA_LOG_MIN.txt 文件为http访问请求状态，完整数据来源：http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html 或者 （链接：https://pan.baidu.com/s/1mi04sys 密码：3max），本例中只使用了部分数据。 <br>
Regularization.py:对信息进行规则化处理，此过程借用了spark中的ROW方法，日后可根据返回数据类型做更改。 <br>
DparkAnalysis.py:使用dpark对信息进行map reduce操作。 <br>
StatisticAnalysis.py:进行各种分析，详见代码注释 <br>
StatisticAnalysis404.py:专门对404状态url进行分析。 <br>
 
