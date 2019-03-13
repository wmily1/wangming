# conding=GBK
# -*- coding: utf-8 -*
'''
库主要用户GA数据的获取
@author: 王铭
@contact: 163664562@qq.com
@file: GA_info.py
@time: 2019/03/12 14:38
'''

import sys
import datetime,time
from config import get_config
from warehouse import DataWareHouse
class GAInfo(get_config,DataWareHouse):
    """
    连接数据库
    """
    def __init__(self):
        super(GAInfo,self).__init__()

    def GetGADataIDList(self,time1,time2):
        """
        根据时间段获取ga_data时间ID列表
        :param time1: 从time1时间开始
        :param time2: 到time2时间结束
        :return:集合 （{'id':ID,'time':datatime},{'id':ID,'time':datatime}）
        """
        sql = "select * from ga_date where time>'{0}' and time<='{1}'".format(time1,time2)
        self.cursor.execute(sql)
        data=self.cursor.fetchall()
        return data

    def GetGAID_Data(self,ID):
        """
        根据id获得时间
        :param ID: 
        :return: 时间点
        """
        sql = "select * from ga_date where id={0}".format(ID)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        if data==():
            return False
        else:
            return [a['time'] for a in data][0]

    def GetGACatalogName_ID(self,ID):
        """
        通过ID获得分类名称
        :return: 
        """
        sql = "select * from ga_catalog where id={0}".format(ID)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        if data == ():
            return False
        else:
            return data

    def GetCatalogName_ID(self,ID):
        """
        通过ID获得catalog的名称
        :param ID: 
        :return: 
        """
        sql = "select * from catalog where catalog_id={0}".format(ID)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        if data == ():
            return False
        else:
            return [a['catalog_name'] for a in data][0]

    def GetCountryName_ID(self,ID):
        """
        通过ID获得国家代码
        :param ID: 
        :return: 
        """
        sql = "select * from core_website where entity_id={0}".format(ID)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        if data == ():
            return False
        else:
            return [a['code'] for a in data][0]

    def GetGACatalogTraffic(self,time1,time2):
        """
        根据时间请求到ga_catalog_traffic的数据
        :param time1: 从time1时间开始
        :param time2: 到time2时间结束
        :return: 数据集合
        """
        time1=(datetime.datetime.now()+datetime.timedelta(-time1)).strftime('%Y-%m-%d')
        time2=(datetime.datetime.now()+datetime.timedelta(-time2)).strftime('%Y-%m-%d')
        data=self.GetGADataIDList(time1,time2)
        time_id=[str(a['id']) for a in data]
        sql="select * from ga_catalog_traffic where date in ({0})".format(','.join(time_id))
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data


if __name__=="__main__":
    GAInfo=GAInfo()


