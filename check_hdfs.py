import sys
import datetime
from tableInfo import TableInfo

class CheckhdfsFile(object):
    def __init__(self,cfg_file):
        self.cfg_file = cfg_file
        self.tableInfos = []
        self.parse_cfg_file()

    def parse_cfg_file(self):
        try:
            fh = open(self.cfg_file,'r')
            line_num = 0
            self.table_infos = []

            for line in fh:
                if line_num == 0:
                    self.header = header = line.strip('\n')
                    header_list = header.split('\t')
                    #print len(header_list)
                    #for value in header_list:
                        #print value
                    #print header_list
                else:
                    info = line.strip('\n')
                    info_list = info.split('\t')
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

                        #print table_info
                    self.table_infos.append(table_info_lists)
                line_num += 1
                #print line,
        except IOError:
            print self.cfg_file + ",file not found!"

if __name__ == '__main__':
    chf = CheckhdfsFile("table.cfg")
    #chf.parse_cfg_file()
    print chf.tableInfos