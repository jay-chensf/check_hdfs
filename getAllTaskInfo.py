#coding=utf-8

import os
import re
from taskinfo import TaskInfo
lineSep = os.linesep

class GetAllTaskInfo():
    def __init__(self,cfg_file):
        self.cfg_file = cfg_file
        # 保存配置任务的信息
        self.taskInfos = []

        self.parse_cfg_file()

    def parse_cfg_file(self):
        """解析配置文件，并保存配置任务的信息"""
        try:
            fh = open(self.cfg_file, 'r')
            line_num = 0

            for line in fh:
                if line_num == 0:
                    self.header = header = line.strip('\n')
                    header_list = header.split('|')
                else:
                    info = line.strip('\n')
                    info_list = info.split('|')

                    for i in range(len(header_list)):
                        if header_list[i] == 'task_name':
                            task_name = info_list[i]
                        elif header_list[i] == 'table_name':
                            table_name = info_list[i]
                        elif header_list[i] == 'workflow_name':
                            workflow_name = info_list[i]
                        elif header_list[i] == 'data_from_table':
                            data_from_table = info_list[i]

                    taskInfo = TaskInfo(task_name, table_name, workflow_name,data_from_table)
                    self.taskInfos.append(taskInfo)

                line_num += 1
                #print line,line_num
        except IOError:
            print self.cfg_file + ",file not found!"

        fh.close()

    def taskRelateExecSizeInfo(self,sizeFile,taskExecFile,resultWriteFile):
        ofh = open(sizeFile, 'r')
        taskfh = open(taskExecFile, 'r')
        lineInfos = ofh.readlines()
        taskLineInfos = taskfh.readlines()
        wfh = open(resultWriteFile, 'w')

        allTasksSizeStr = ""
        for taskinfo in self.taskInfos:
            allTasksSizes = []
            curTableInfo = ""
            dataFromTableSize = 0
            print(taskinfo.table_name + "," + taskinfo.task_name + "," + taskinfo.workflow_name + "," + taskinfo.data_from_table)
            for line in lineInfos:
                if taskinfo.table_name in line:
                    tableSizeInfo = line.split("|")
                    # tableName = tableSizeInfo[0]
                    cycle = tableSizeInfo[2]
                    timePeriod = tableSizeInfo[3]
                    tableSize = tableSizeInfo[4]
                    curTableInfo = taskinfo.table_name + "|" + taskinfo.task_name + "|" + taskinfo.workflow_name + "|" + cycle + "|" + timePeriod + "|" + tableSize + "|" + taskinfo.data_from_table
                    dataFromTable = taskinfo.data_from_table.split('|')
                    #取到当前任务表的信息后，继续遍历获取来源表的数据量大小
                    for fromLine in lineInfos:
                        for dft in dataFromTable:
                            if dft in fromLine:
                                if cycle == 'MI':
                                    fromTableSizeInfo = fromLine.split("|")
                                    fromTimePeriod = fromTableSizeInfo[3]
                                    fromTableSize = fromTableSizeInfo[4]
                                    dataFromTableSize += int(fromTableSize)
                else:
                    for dataFromTable in taskinfo.data_from_table.split('|'):
                        if dataFromTable in line:
                            if taskinfo.cycle == 'MI':
                                fromTableSize = line.split("|")[4]
                                dataFromTableSize += int(fromTableSize)
            if len(curTableInfo) > 0:
                allTasksSizes.append(curTableInfo + "|" + str(dataFromTableSize))

            for tLine in taskLineInfos:
                if taskinfo.table_name in tLine:
                    for tss in allTasksSizes:
                        taskExecInfo = tLine.split("|")
                        execTimePeriod = taskExecInfo[1]
                        execStartTime = taskExecInfo[2]
                        execEndTime = taskExecInfo[3]
                        execUsedTime = taskExecInfo[4]
                        execRecordNum = taskExecInfo[6]
                        if execTimePeriod == tss.split("|")[4]:
                            if(tss.strip('\n')) in allTasksSizeStr:
                                oriStrReg = tss.strip('\n') + '.*' + lineSep
                                oriStrReg = oriStrReg.replace("|","\|")
                                replaceStr = tss.strip('\n') + "|" + execStartTime + "|" + execEndTime + "|" + execUsedTime + "|" + execRecordNum + lineSep
                                allTasksSizeStr = re.sub(r'' + oriStrReg, replaceStr, allTasksSizeStr)
                                #print re.sub(r'' + oriStrReg, replaceStr, allTasksSizeStr)
                            else:
                                allTasksSizeStr += tss.strip('\n') + "|" + execStartTime + "|" + execEndTime + "|" + execUsedTime + "|" + execRecordNum + lineSep


        wfh.write(allTasksSizeStr)
        wfh.close()
        taskfh.close()
        ofh.close()

if __name__ == '__main__':
    gati = GetAllTaskInfo("allTask.cfg")
    #gati.parse_cfg_file()
    #print(gati.taskInfos)
    gati.taskRelateExecSizeInfo("D:/PythonProject/table_size.txt", "D:/PythonProject/task_exec_info.txt", "D:/PythonProject/final_task.txt")
    """
    testStr = "ODS_RE_ST_XDR_S1MME_DW_RE_ST_XDR_S1U_DNS_PERF_WIDE_H|DW_RE_ST_XDR_S1U_DNS_PERF_WIDE_H|LTE_DNS_TASK|H|2019052214|498926718|20190522181015|20190522190143|3088|19596863" + lineSep
    testStr += "ODS_RE_ST_XDR_S1U_VIDEO_HDET_H|ODS_RE_ST_XDR_S1U_VIDEO_HDET_H|LTE_VIDEO_TASK|H|2019052221|1644271105|20190522230203|20190522231011|488|22630569" + lineSep
    #print testStr
    regStr = 'ODS_RE_ST_XDR_S1MME_DW_RE_ST_XDR_S1U_DNS_PERF_WIDE_H\|DW_RE_ST_XDR_S1U_DNS_PERF_WIDE_H\|LTE_DNS_TASK\|H\|2019052214\|498926718' +  '111111' + lineSep
    #print re.sub(r'' + regStr,'444444',testStr)
    #print testStr
    replaceStr = 'ODS_RE_ST_XDR_S1MME_DW_RE_ST_XDR_S1U_DNS_PERF_WIDE_H|DW_RE_ST_XDR_S1U_DNS_PERF_WIDE_H|LTE_DNS_TASK|H|2019052214|498926718|20190522221015|20190522230143|1111|222222' + lineSep
    testStr = re.sub(r'' + regStr,replaceStr,testStr)
    print testStr"""

