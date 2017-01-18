# -*- coding: UTF-8 -*-

from pyspark import  SparkContext
from datetime import *
import shutil,os


#filter invaild line
def filterline(line):
    for item in line[1]:
        if len(item) == 8:
            continue
        return True
    return False

def fine_weather(line,time):
    time_delta = 0
    index_f = 0
    #print "time_input:-------",time
    time_user = datetime.strptime(str(time),"%Y-%m-%d %H:%M")
    #print "time_user:",time_user
    for index,item in enumerate(line):
        if len(item) == 8:
            #print "item in line:",item
            time_weather_str = str(item[1]+" "+item[2])
            #print "time_weather_str:",time_weather_str
            time_delta_tmp = 0
            time_weather = datetime.strptime(time_weather_str,"%Y-%m-%d %H:%M:%S")
            #print "time_weather:",time_weather
            if time_weather > time_user:
                time_delta_tmp = (time_weather-time_user).seconds
            else:
                time_delta_tmp = (time_user-time_weather).seconds
            #print "time_delta_tmp:",time_delta_tmp
            if time_delta == 0 and index_f == 0:
                #print "init time_delta,index_f"
                time_delta = time_delta_tmp
                index_f = index
            if time_delta_tmp < time_delta:
                #print "index:++++++",index
                index_f = index
                time_delta = time_delta_tmp
    return line[index_f][1:]


def mapone(line):
    line_list = []
    for item in line:
        if len(item) != 8:
            weather = fine_weather(line,item[3])
            #print "weather:",weather
            #print "item:",item
            item.extend(weather)
            #print "item:",item
            line_list.append(item)
    #print "line_list++++++:",line_list
    return line_list

def map_list_string(record):
    for item in record:
        string = u','.join(item).encode('utf8')
    return string


if __name__ == "__main__":
    file_weather = "./china_dec_weather.txt"
    file_input = "./input"

    sc = SparkContext("local[8]", "Etlstart Spark App")

    if os.path.exists('./output_weather'):#modifyed by Xueping
        shutil.rmtree('./output_weather') #modifyed by Xueping

    data_weather = sc.textFile(file_weather).map(lambda line: line.split(" ")).\
    map(lambda line:(line[0]+" "+line[1],line)).cache()

    data_tem = sc.textFile(file_input).map(lambda line:line.split(",")).\
    map(lambda line:(line[-1]+" "+line[3][0:10],line)).union(data_weather).\
    groupByKey().mapValues(list).filter(filterline).map(lambda line:line[1]).\
    flatMap(mapone).map(lambda line:u",".join(line).encode('utf8')).\
    saveAsTextFile("./output_weather")

    #foreach(lambda line:print line)#map(lambda line:u"\n".join(line))#.map(lambda line:(1,line))#.map(map_list_string)#\

    #print data_tem.take(10)
    sc.stop()
