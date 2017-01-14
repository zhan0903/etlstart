# -*- coding: UTF-8 -*-
from pyspark import  SparkContext
import os,traceback,random,codecs
#import sys,chardet

def process(files):
    for item in files:
        processfile(item)

def processfile(record):
    # for item in record:
    #     print "in processrecord:++++",item
    #     print "record over++++++++++++++++++++++++"
    #print "in processrecord:++++",record
    #print "record over++++++++++++++++++++++++"
    data = []
    line_list = record[1].split('\n')
    user=country=province=city = ""
    #seqcount = 0
    xulie = False
    data_record = []
    count = 0

    for item in line_list:#for each user information
        if item == '':
            break
        item_list = item.split(",")#change user information to list
        if len(item_list) != 9:
            print "len(item_list):",len(item_list)
            print "type:",type(item_list)
            print "item:",item_list.encode('utf8')
            raise ValueError("INPUT RECORD ERROR!")
        check = item_list[0]
        if check == "/":#one device content
            data_time = item_list[8].split(' ')
            if item_list[2] == "poweron":
                end_time = item_list[8]
                xulie = True
                if count == 0:#seq first one 序列第一次
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
                #print item_list
                country=province=city=""
                continue
            address = item_list[1]
            country = address[0:2]#.encode('utf8').decode('utf8')
            if address[2:4] == u"黑龙" or address[2:4] == u"内蒙":
                province = address[2:5]#.encode('utf8').decode('utf8')
                city = address[5:]#.encode('utf8').decode('utf8')
            else:
                province = address[2:4]#.encode('utf8').decode('utf8')
                city = address[4:]#.encode('utf8').decode('utf8')

    accum.add(1)
    #print "accum:",accum,type(accum)
    filename = "./outtest/%s"%accum
    with codecs.open('results.txt',"a+","utf-8") as f1:
    #print data >> f1
        #print "data:",data
        for item in data:
            if len(item) != 11:
                print "len(item):",len(item)
                print "type:",type(item)
                print "item:",item.encode('utf8')
                raise ValueError("INPUT RECORD ERROR!")
            string = ','.join(item).encode('utf8')
            #print "string:",string
            if len(string.split(',')) != 11:
                print "len(item):",len(item)
                print "type:",type(item)
                print "item:",item.encode('utf8')
                raise ValueError("INPUT RECORD ERROR!")
            print >> f1,string.decode('utf8')
            #f1.write(str(item))
            #f1.write(u"\n")

def map_list_string(record):
    for item in record:
        string = u','.join(item).encode('utf8')
    return string

def filterone(line):
    try:
        if int(line[-1]) > 1:
            return True
        else:
            return False
    except Exception, e:
        print "line--------:",line


file_dir = "./csv"
data = []
sc = SparkContext("local[4]", "First Spark App")
#raw_data = sc.textFile(file_dir)
#data = sc.textFile(file_dir).map(lambda line: line.split(",")).map(lambda record: (record[0], record[1], \
#record[2],record[3],record[4],record[5],record[6],record[7],record[8]))
#line_list = data.collect()
accum = sc.accumulator(0)
raw_data = sc.wholeTextFiles(file_dir)
partitions = raw_data.getNumPartitions()
print "partitions=============:",partitions

#open('./results.txt','w').close()
if os.path.exists('./results.txt'):
    os.remove('./results.txt')


raw_data.foreachPartition(process)
#print "--------",data_new
#sc.stop()

#data_out = sc.textFile('./results.txt').map(lambda line: line.split(",")).\
#filter(lambda line: int(line[-1]) > 1).\
#countByKey()
#------
# data_out = sc.textFile('./outtest').map(lambda line: line.split(",")).\
# filter(filterone).map(lambda line:(line[0],line)).\
# groupByKey().mapValues(list).filter(lambda item:len(item[1]) > 3).\
# flatMapValues(lambda item: item).map(map_list_string).saveAsTextFile("./output")
#------
#print data_out.take(10)
#record[2],record[3],record[4],record[5],record[6],record[7],record[8]))
#print data_new.take(10)
#data_sc = sc.parallelize(data_new)
#print "-----------",data_new.take(1)
#data_out = data_new.map(lambda x:(x[0],1))
#print data_out.take(5)
#####data_out_f =  data_out.reduceByKey(lambda a,b:a+b).saveAsTextFile("./output")
#print "+++++++++",data_out_f
#data_final += data_out_f.collect()
#print data_out_f.take(10)
#sc.write
sc.stop()
#exit(0)
#print data.take(10)
#print line_list
#print data
