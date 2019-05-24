#coding=utf-8

import os
import sys
import datetime
from tableInfo import TableInfo
import re

lineSep = os.linesep

class CheckhdfsFile(object):
    def __init__(self,cfg_file):
        self.cfg_file = cfg_file
        #保存配置表的信息
        self.tableInfos = []

        self.parse_cfg_file()
        #保存配置文件中所有表的数据路径
        self.all_hdfspaths = ''

    def parse_cfg_file(self):
        """解析配置文件，并保存配置表的信息"""
        try:
            fh = open(self.cfg_file,'r')
            line_num = 0

            for line in fh:
                if line_num == 0:
                    self.header = header = line.strip('\n')
                    header_list = header.split('|')
                else:
                    info = line.strip('\n')
                    info_list = info.split('|')
                    table_info_lists = []
                    for i in range(len(header_list)):
                        if header_list[i] == 'table':
                            name = info_list[i]
                        elif header_list[i] == 'path':
                            path = info_list[i]
                        elif header_list[i] == 'cycle':
                            cycle = info_list[i]
                        elif header_list[i] == 'check_delay':
                            check_delay = info_list[i]
                        elif header_list[i] == 'check_time':
                            check_time = info_list[i]

                    tableInfo = TableInfo(name,path,cycle,check_delay,check_time)
                    self.tableInfos.append(tableInfo)

                line_num += 1
                #print line,
        except IOError:
            print self.cfg_file + ",file not found!"

        fh.close()

    def AddMonths(self,d, x):
        """月份加减函数"""
        newmonth = (((d.month - 1) + x) % 12) + 1
        newyear = d.year + (((d.month - 1) + x) / 12)
        return datetime.datetime(newyear, newmonth, d.day,d.hour,d.minute,d.second,d.microsecond)

    def getTableHDFSPath(self):
        """根据配置表的信息及当前时间获取需要检查的数据路径信息"""
        timenow = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        print ("timeNow_str : " + timenow)
        for ti in self.tableInfos:
            if (ti.cycle == 'MI'):
                checktime_str = (datetime.datetime.now() + datetime.timedelta(minutes=-int(ti.check_delay))).strftime(
                    "%Y%m%d%H%M%S")
                hour_str = ''
                for hour in range(24):
                    if hour < 10:
                        hour_str = '0' + str(hour)
                    else:
                        hour_str = str(hour)
                    for mi_str in ['00','15','30','45']:
                        checkPath = str(ti.path).replace('yyyymmdd', checktime_str[0:8]).replace('hh_hh',hour_str).replace('mi_mi', mi_str)
                        self.all_hdfspaths += " " + checkPath
            elif (ti.cycle == 'H'):
                checktime_str = (datetime.datetime.now() + datetime.timedelta(hours=-int(ti.check_delay))).strftime(
                    "%Y%m%d%H%M%S")
                hour_str = ''
                for hour in range(24):
                    if hour < 10:
                        hour_str = '0' + str(hour)
                    else:
                        hour_str = str(hour)
                    checkPath = str(ti.path).replace('yyyymmdd', checktime_str[0:8]).replace('hh_hh', hour_str)
                    self.all_hdfspaths += " " + checkPath
            elif (ti.cycle == 'D'):
                checktime_str = (datetime.datetime.now() + datetime.timedelta(days=-int(ti.check_delay))).strftime(
                    "%Y%m%d%H%M%S")
                checkPath = str(ti.path).replace('yyyymmdd', checktime_str[0:8])
                self.all_hdfspaths += " " + checkPath
            elif (ti.cycle == 'M'):
                checktime_str = self.AddMonths(datetime.datetime.now(), -int(ti.check_delay)).strftime(
                    "%Y%m%d%H%M%S")
                checkPath = str(ti.path).replace('yyyymm', checktime_str[0:6])
                self.all_hdfspaths += " " + checkPath

    def getTablePathSizeInfo(self,tableSizePath,resultPath):
        allInfos = ""
        fh = open(tableSizePath,'r')
        resultFile = resultPath + "/" + self.lastHourCheckTime + ".txt"
        wfh = open(resultFile, 'w')
        lineInfos = fh.readlines()
        for ti in self.tableInfos:
            tableHdfsPath = ti.path.split('/yyyymm')[0]
            #print("tableHdfsPath : " + tableHdfsPath)
            for line in lineInfos:
                if tableHdfsPath in line:
                    lineInfo = re.split(r'\s+',line.strip())
                    #print(lineInfo)
                    if len(lineInfo) == 2 and lineInfo[0].isdigit() :
                        #print(lineInfo[1])
                        timePeriod = lineInfo[1].split(tableHdfsPath)[1].replace('/', '')
                        #print (ti.name + "|" + lineInfo[1] + "|" + ti.cycle + "|" + timePeriod + "|" + lineInfo[0])
            wfh.write(allInfos)
            wfh.close()
            fh.close()

if __name__ == '__main__':
    chf = CheckhdfsFile("table.cfg")
    #chf.parse_cfg_file()
    #print chf.tableInfos
    chf.getTableHDFSPath()
    wfh = open("D:/PythonProject/test_path.txt", 'w')
    wfh.write(chf.all_hdfspaths)
    wfh.close()
    #print(chf.all_hdfspaths)
    #执行 hdfs fs du -s 命令获取表数据路径下的数据大小，并将结果保存到文件中
    #将配置表的信息和获取到的表数据路径数据大小信息关联保存
    #chf.getTablePathSizeInfo("D:/PythonProject/table_size.txt")
