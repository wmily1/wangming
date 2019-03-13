# conding=GBK
# -*- coding: utf-8 -*
'''
库主要用于操作data_warehouse数据
@author: 王铭
@contact: 163664562@qq.com
@file: warehouse.py
@time: 2019/03/12 14:38
'''

import sys
import datetime,time

class DataWareHouse():
    """
    连接data_warehouse数据库
    """
    def __init__(self):
        self.db_warehouse=self.db_warehouse
        self.cursor_warehouse=self.cursor_warehouse

    def GetWareHouseWebsiteId(self,name):
        """
        根据国家名字获得国家ID
        :param name: 
        :return: 
        """
        sql="select * from public_website_dim where name='{}'".format(name)
        self.cursor_warehouse.execute(sql)
        data = self.cursor_warehouse.fetchall()
        if data == ():
            return False
        else:
            return [a['website_sk'] for a in data][0]

    def GetWareHouseTimeId(self,time):
        """
        根据时间获得时间ID
        :param time: 
        :return: 
        """
        sql = "select * from public_times_dim where time_D='{}'".format(time)
        self.cursor_warehouse.execute(sql)
        data = self.cursor_warehouse.fetchall()
        if data == ():
            return False
        else:
            return [a['time_sk'] for a in data][0]


    def GetWareHouseCatalogId(self,name):
        """
        根据名称获得时间分类ID
        :param name: 
        :return: 
        """
        sql = "select * from retail_product_dim where three_catalog='{}'".format(name)
        self.cursor_warehouse.execute(sql)
        data = self.cursor_warehouse.fetchall()
        if data == ():
            sql = "select * from retail_product_dim where secondary_catalog='{}'".format(name)
            self.cursor_warehouse.execute(sql)
            data = self.cursor_warehouse.fetchall()
            if data==():
                sql = "select * from retail_product_dim where first_catalog='{}'".format(name)
                self.cursor_warehouse.execute(sql)
                data = self.cursor_warehouse.fetchall()
                if data==():
                    return False
                else:
                    return [a['product_sk'] for a in data][0]
            else:
                return [a['product_sk'] for a in data][0]
        else:
            return [a['product_sk'] for a in data][0]


    def InputWareHouseTrafiicCatalogFact(self,data):
        """
        插入数据到trafiic_catalog_fact
        :param data: 
        :return: 
        """
        sql = "INSERT INTO trafiic_catalog_fact (UV, newuser,bouncerate,product_sk,time_sk,website_sk) VALUES ({0},{1},{2},{3},{4},{5})".format(
            data[0],data[1],data[2],data[3],data[4],data[5])
        self.cursor_warehouse.execute(sql)
        self.db_warehouse.commit()

    def UpdateWareHouseTrafiicCatalogFact(self,data):
        """
        更新数据到trafiic_catalog_fact
        :return: 
        """
        sql = "update trafiic_catalog_fact set UV={0}, newuser={1},bouncerate={2} where product_sk={3} and time_sk={4} and website_sk={5}".format(
            data[0], data[1], data[2], data[3], data[4], data[5])
        self.cursor_warehouse.execute(sql)
        self.db_warehouse.commit()

    def SelectWareHouseTrafiicCatalogFact(self,data):
        """
        选择数据到trafiic_catalog_fact
        :return: 
        """
        sql = "select * from trafiic_catalog_fact where product_sk={0} and time_sk={1} and website_sk={2}".format(
            data[3], data[4], data[5])
        self.cursor_warehouse.execute(sql)
        data=self.cursor_warehouse.fetchall()
        return data

    def UpdateORinputWareHouseTrafiicCatalogFact(self,data):
        """
        更新或者创建
        :param data: 列表
        :return:
        """
        if self.SelectWareHouseTrafiicCatalogFact(data)==():
            self.InputWareHouseTrafiicCatalogFact(data)
        else:
            self.UpdateWareHouseTrafiicCatalogFact(data)
