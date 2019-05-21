#coding=utf-8

import os
import datetime
from check_hdfs import CheckhdfsFile

programPath = "/app/boco/chensf/checkHDFSPathSize/"
chf = CheckhdfsFile(programPath + "table.cfg")
chf.getTableHDFSPath()
#执行 hdfs fs du -s 命令获取表数据路径下的数据大小，并将结果保存到文件中
checkSizeFilePath = programPath + "checkSizeFile/"
timeNowStr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
hdfsPathSizeFile = checkSizeFilePath + timeNowStr + ".txt"
tableResultPath = programPath + "resultFile"
os.system("source /app/boco/TDH-Client/init.sh")
os.system("hadoop fs -du -s " + chf.all_hdfspaths + " > " + hdfsPathSizeFile + " 2>&1")
#将配置表的信息和获取到的表数据路径数据大小信息关联保存
chf.getTablePathSizeInfo(hdfsPathSizeFile,tableResultPath)