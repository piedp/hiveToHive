#coding:utf-8

import sys
import os
from pyhive import hive


field_sep_token = b'\x7C\x1C'

class Table(object):

    def __init__(self, ip, port, username, password,auth, schema, tablename,columns):
        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
        self.schema = schema
        self.tablename = tablename
        self.auth = auth
        self.columns = columns
        self.conn = hive.Connection(host=ip,port=port,username=username,password=password,auth=auth)

    def exportTable(self, columns):
        '''
        将table中的数据导出成.dat文件
        columns:要导出的列
        :return:
        '''
        cursor = self.conn.cursor()
        #拼接sql
        sqlStr = 'select'
        for i in range(len(columns)):
            if i != len(columns) - 1:
                sqlStr = sqlStr + ' ' + columns[i] + ','
            else:
                sqlStr = sqlStr + ' ' + columns[i]

        sqlStr= sqlStr + ' from ' + self.schema + '.' + self.tablename

        dataFile = open('../data/' + self.tablename + '.dat','wb+')

        cursor.execute(sqlStr)
        while True:
            data = cursor.fetchmany(size=1000)
            if len(data) == 0:
                break
            for tup in data:
                temp = []
                for row in tup:
                    temp.append(str(row).encode('utf-8'))
                temp.append(b'\n')
                line = field_sep_token.join(temp)
                dataFile.write(line)

        dataFile.close()
    

    def importTable(self):
        '''
        将数据导入到table中
        columns
        :return:
        '''
        pass