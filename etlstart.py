# -*- coding: UTF-8 -*-
#原始数据文件放在当前目录下的csv目录下，数据清洗最终结果会放在当前目录下的output目录下
#程序运行方式（命令行方式）：／path／spark-summit etlstart.py

from pyspark import  SparkContext
import os,codecs

#生成中间文件（类似于数据库中格式），并存储到results.txt文件中
def processfile(record):
    data = []
    line_list = record[1].split('\n')
    user=country=province=city = ""
    xulie = False
    data_record = []
    count = 0

    for item in line_list:#for each user information
        if item == '':
            break
        item_list = item.split(",")#change user information to list
        check = item_list[0]
        if check == "/":#one device content
            data_time = item_list[8].split(' ')
            if item_list[2] == "poweron":
                end_time = item_list[8]
                xulie = True
                if count == 0:#seq first one
                    data_record = [user,province,city,item_list[3],item_list[4],\
                    item_list[5],item_list[6],item_list[7],item_list[8],]#add start_time
                count += 1
            else:#poweroff
                if xulie:
                    data_record.append(end_time)#add end_time
                    data_record.append(str(count))#add count
                    data.append(data_record)#add one record
                    xulie = False
                    count = 0
        else:# another device id
            user = check[3:]
            if item_list[1] == "None":
                country=province=city=""
                continue
            address = item_list[1]
            country = address[0:2]
            if address[2:4] == u"黑龙" or address[2:4] == u"内蒙":
                province = address[2:5]
                city = address[5:]
            else:
                province = address[2:4]
                city = address[4:]

    with codecs.open('results.txt',"a+","utf-8") as f1:
        for item in data:
            string = ','.join(item).encode('utf8')
            print >> f1,string.decode('utf8')

#从列表转换为str类型
def map_list_string(record):
    for item in record:
        string = u','.join(item).encode('utf8')
    return string

#过滤序列长度小于2的项
def filterone(line):
    try:
        if int(line[-1]) > 1:
            return True
        else:
            return False
    except Exception, e:
        print line

if __name__ == "__main__":
    file_dir = "./csv"
    sc = SparkContext("local[4]", "First Spark App")
    raw_data = sc.wholeTextFiles(file_dir)
    partitions = raw_data.getNumPartitions()

    if os.path.exists('./results.txt'):
        os.remove('./results.txt')
    raw_data.foreach(processfile)

    data_out = sc.textFile('./results.txt').map(lambda line: line.split(",")).\
    filter(filterone).map(lambda line:(line[0],line)).\
    groupByKey().mapValues(list).filter(lambda item:len(item[1]) > 3).\
    flatMapValues(lambda item: item).map(map_list_string).saveAsTextFile("./output")

    sc.stop()
