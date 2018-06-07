# !/usr/bin/env python3
# encoding: utf-8
"""
@version: v1.0
@author: W_H_J
@license: Apache Licence
@contact: 415900617@qq.com
@site:
@software: PyCharm
@file: StatisticAnalysis.py
@time: 2018/6/6 16:07
@describe:统计分析
"""

from DparkAndSpark.DparkAnalysis import access_logs
import matplotlib.pyplot as plt


# 1-对web日志返回的结果做统计，返回内容大小,返回服务器返回的数据大小TOP5。
def viewCount():
    content_sizes = access_logs.map(lambda log: log.content_size).cache()
    print('1===>Content Size Avg: %i,Top 5: %s' % (
        content_sizes.reduce(lambda a, b: a + b) / content_sizes.count(), content_sizes.top(5),))


# 2-返回状态码分析
def vieWCode():
    responseCodeToCount = (access_logs
                           .map(lambda log: (log.response_code, 1))
                           .reduceByKey(lambda a, b : a + b)
                           .cache())
    responseCodeToCountList = responseCodeToCount.take(100)
    # 返回状态码类型统计
    print('2===>Found %d response codes' % len(responseCodeToCountList))
    # 统计返回不同类型状态码条数
    print('3===>Response Code Counts: %s' % responseCodeToCountList)

    # 返回状态码类型统计-图形绘制
    def pie_pct_format(value):
        """ Determine the appropriate format string for the pie chart percentage label
        Args:
            value: value of the pie slice
        Returns:
            str: formated string label; if the slice is too small to fit, returns an empty string for label
        """
        return '' if value < 7 else '%.0f%%' % value

    labels = responseCodeToCount.map(lambda s: s[0]).collect()
    count = access_logs.count()
    fracs = responseCodeToCount.map(lambda s: (float(s[1]) / count)).collect()
    fig = plt.figure(figsize=(10, 10), facecolor='white', edgecolor='white')
    colors = ['yellowgreen', 'lightskyblue', 'gold', 'purple', 'lightcoral', 'yellow', 'black']
    explode = (0.05, 0.05, 0.1, 0, 0)
    patches, texts, autotexts = plt.pie(fracs, labels=labels, colors=colors,
                                        explode=explode, autopct=pie_pct_format,
                                        shadow=False, startangle=125)
    for text, autotext in zip(texts, autotexts):
        if autotext.get_text() == '':
            text.set_text('')  # If the slice is small to fit, don't show a text label
    plt.legend(labels, loc=(0.80, -0.1), shadow=True)
    plt.savefig('./img/code.png')

    # 不同类型状态码
    labels = responseCodeToCount.map(lambda s: s[0]).collect()
    print("4===>Codes:", labels)
    # 所占比例
    count = access_logs.count()
    fracs = responseCodeToCount.map(lambda s: (float(s[1]) / count)).collect()
    print("5===>Ratio:", list(fracs))
    # 百分比
    print("6===>percentage:", dict(zip(labels,["{:.2%}".format(round(x,3)) for x in list(fracs)])))


# 3-服务器访问频次超过10次以上
def viewCountSum():
    # 任何访问服务器的主机超过10次。.
    hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))

    hostSum = hostCountPairTuple.reduceByKey(lambda a, b: a + b)

    hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)

    hostsPick20 = (hostMoreThan10
                   .map(lambda s: s[0])
                   .take(20))

    print('7===>Any 20 hosts that have accessed more then 10 times: %s' % hostsPick20)

    endpoints = (access_logs
                 .map(lambda log: (log.endpoint, 1))
                 .reduceByKey(lambda a, b: a + b)
                 .cache())
    ends = endpoints.map(lambda s: s[0]).collect()
    counts = endpoints.map(lambda s: s[1]).collect()
    print(ends)
    print(counts)
    # 图像绘制
    fig = plt.figure(figsize=(8, 4.2), facecolor='white', edgecolor='white')
    plt.axis([0, len(ends), 0, max(counts)])
    plt.grid(b=True, which='major', axis='y')
    plt.xlabel('Endpoints')
    plt.ylabel('Number of Hits')
    plt.plot(counts)
    plt.savefig("./img/Endpoints.png")


# 4-统计访问hosts
def viewHostsCount():
    # 不同的Host个数统计
    hosts = access_logs.map(lambda log: (log.host, 1))
    uniqueHosts = hosts.reduceByKey(lambda a, b: a + b)
    uniqueHostCount = uniqueHosts.count()
    print('8===>Unique hosts: %d' % uniqueHostCount)

    # 每一天有多少不同的Host
    dayToHostPairTuple = access_logs.map(lambda log: (log.date_time.day, log.host))
    dayGroupedHosts = dayToHostPairTuple.groupByKey()
    dayHostCount = dayGroupedHosts.map(lambda s: (s[0], len(set(s[1]))))
    # spark 中使用sortByKey，但是dpark中没有该方法，暂时不知替代方法时什么
    # dailyHosts = dayHostCount.sortByKey().cache()
    dailyHosts = dayHostCount.groupByKey().sort().cache()
    dailyHostsList = dailyHosts.take(30)
    print('9===>Unique hosts per day: %s' % dailyHostsList)

    # 统计每一天单个Host平均发起的请求数
    dayAndHostTuple = access_logs.map(lambda log: (log.date_time.day, log.host))
    groupedByDay = dayAndHostTuple.groupByKey()
    # spark 中使用sortByKey，但是dpark中没有该方法，所以用groupBykey().sort()实现
    sortedByDay = groupedByDay.sort()
    avgDailyReqPerHost = sortedByDay.map(lambda s: (s[0], len(s[1]) / len(set(s[1])))).cache()
    avgDailyReqPerHostList = avgDailyReqPerHost.take(30)
    print('10===>Average number of daily requests per Hosts is %s' % avgDailyReqPerHostList)


# 5-统计404访问
def viewFind404():
    badRecords = (access_logs
                  .filter(lambda log: log.response_code == 404)
                  .cache())
    print('11===>Found %d 404 URLs' % badRecords.count())
    # 404urls
    badEndpoints = badRecords.map(lambda log: log.endpoint)
    # 去重操作，但是dpark没有该方法
    # badUniqueEndpoints = badEndpoints.distinct()
    # badUniqueEndpointsPick40 = badUniqueEndpoints.take(10)
    # 所以使用list数据结构中的set进行去重
    badUniqueEndpointsPick40 = list(set(badEndpoints.take(10)))
    print('12===>404 URLS: %s' % badUniqueEndpointsPick40)

    # top20 404 返回
    badEndpointsCountPairTuple = badRecords.map(lambda log: (log.endpoint, 1))
    badEndpointsSum = badEndpointsCountPairTuple.reduceByKey(lambda a, b: a + b)
    '''
    # 降序排序
    spark方法：
    badEndpointsTop20 = badEndpointsSum.takeOrdered(20, lambda s: -1 * s[1])
    '''
    # dpark
    badEndpointsTop20 = badEndpointsSum.sort(lambda s: -1 * s[1]).take(20)
    print('13===>Top-20 404 URLs: %s' % badEndpointsTop20)
    # 找出前25个返回404的host
    errHostsCountPairTuple = badRecords.map(lambda log: (log.host, 1))
    errHostsSum = errHostsCountPairTuple.reduceByKey(lambda a, b: a + b)
    errHostsTop25 = errHostsSum.sort(lambda s: -1 * s[1]).take(25)
    print('14===>Top-25 hosts that generated errors: %s' % errHostsTop25)
    # 按天统计404
    errDateCountPairTuple = badRecords.map(lambda log: (log.date_time.day, 1))
    errDateSum = errDateCountPairTuple.reduceByKey(lambda a, b: a + b)
    errDateSorted = (errDateSum
                     .groupByKey().sort()
                     .cache())
    errByDate = errDateSorted.collect()
    print('15===>404 Errors by day: %s' % errByDate)

    topErrDate = errDateSorted.sort(lambda s: -1 * s[1]).take(5)
    print('16===>Top Five dates for 404 requests: %s' % topErrDate)
    # 按小时分类
    hourCountPairTuple = badRecords.map(lambda log: (log.date_time.hour, 1))
    hourRecordsSum = hourCountPairTuple.reduceByKey(lambda a, b: a + b)
    hourRecordsSorted = (hourRecordsSum
                         .groupByKey().sort()
                         .cache())
    errHourList = hourRecordsSorted.collect()
    print('17===>Top hours for 404 requests: %s' % errHourList)
    # 绘制图像
    hoursWithErrors404 = hourRecordsSorted.map(lambda s: s[0]).collect()
    errors404ByHours = hourRecordsSorted.map(lambda s: s[1]).collect()
    fig = plt.figure(figsize=(8, 4.2), facecolor='white', edgecolor='white')
    plt.axis([0, max(hoursWithErrors404), 0, max(x for x in errors404ByHours)[0]])
    plt.grid(b=True, which='major', axis='y')
    plt.xlabel('Hour')
    plt.ylabel('404 Errors')
    plt.plot(hoursWithErrors404, errors404ByHours)
    plt.savefig("./img/hours404.png")


# 统计返回状态不是200的站点
def viewCountNot200():
    not200 = access_logs.filter(lambda x: x.response_code != 200)
    endpointCountPairTuple = not200.map(lambda x: (x.endpoint, 1))
    endpointSum = endpointCountPairTuple.reduceByKey(lambda a, b: a + b)
    topTenErrURLs = endpointSum.sort(lambda s: -1 * s[1]).take(10)
    print('18===>Top Ten failed URLs: %s' % topTenErrURLs)


# 统计唯一的host站点
def viewHostOne():
    hosts = access_logs.map(lambda log: (log.host, 1))
    uniqueHosts = hosts.reduceByKey(lambda a, b: a + b)
    uniqueHostCount = uniqueHosts.count()
    print('19===>Unique hosts: %d' % uniqueHostCount)


# 统计每日访问数量
def viewHostByDay():
    dayToHostPairTuple = access_logs.map(lambda log: (log.date_time.day, log.host))
    dayGroupedHosts = dayToHostPairTuple.groupByKey()
    dayHostCount = dayGroupedHosts.map(lambda s: (s[0], len(set(s[1]))))
    dailyHosts = dayHostCount.sort(lambda item: item[0]).cache()
    dailyHostsList = dailyHosts.take(30)
    print('20===>Unique hosts per day: %s' % dailyHostsList)


# 每个主机的平均请求数
def viewHostByDayAvg():
    dayAndHostTuple = access_logs.map(lambda log: (log.date_time.day, log.host))
    groupedByDay = dayAndHostTuple.groupByKey().map(lambda s: (s[0], int(len(s[1]) * 1.0 / len(set(s[1])))))
    sortedByDay = groupedByDay.sort(lambda item: item[0])
    avgDailyReqPerHost = sortedByDay.cache()
    avgDailyReqPerHostList = avgDailyReqPerHost.take(30)
    print('21===>Average number of daily requests per Hosts is %s' % avgDailyReqPerHostList)


if __name__ == '__main__':
    viewCount()
    vieWCode()
    viewCountSum()
    viewHostsCount()
    viewFind404()
    viewCountNot200()
    viewHostOne()
    viewHostByDay()
    viewHostByDayAvg()


