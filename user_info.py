# conding=GBK
# -*- coding: utf-8 -*
'''
这库主要用于通过邮箱获取用户信息
@author: 王铭
@contact: 163664562@qq.com
@file: user_info.py
@time: 2019/03/08 14:38
'''

import sys
import datetime,time
from config import get_config


class GetUserInfo(get_config):
    """
    获取用户信息
    """
    def __init__(self):
        super(GetUserInfo,self).__init__()


    def Email(self,email):
        """
        初始化邮件名
        :param email: 邮件-字符串
        :return: 获得邮件ID
        """
        self.email=email
        self.__get_email_sql()
        self.product_id=[]




    def __get_email_sql(self):
        """
        获得邮件id
        :return: 
        """
        try:
            sql = "select * from customer_entity where email='%s'" % self.email
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            self.email_id = []
            self.is_register=[]
            for name in results:
                self.email_id.append(name['entity_id'])
                self.is_register.append(name['is_register'])
        except:
            self.logger.info(u'搜索customer_entity数据库报错')
            sys.exit()

    @property
    def EmailID(self):
        """
        外部获得邮箱ID
        :return: 
        """
        return self.email_id


    def __get_buy_product_id_sql(self):
        """
        获取分类id
        :return: 
        """
        try:
            if self.email_id==[]:
                self.product_id=[]
            else:
                sql="select * from sales_order_product where customer_entity_id in (%s)" % ','.join(str(i) for i in self.email_id)
                self.cursor.execute(sql)
                results = self.cursor.fetchall()
                self.product_id = []
                for name in results:
                    self.product_id.append(name['product_id'])
        except:
            self.logger.info(u'搜索sales_order_product数据库出错')
            sys.exit()

    @property
    def BuyProductID(self):
        """
        外部获取客户的订单产品ID
        :return: 
        """
        if self.email_id==[]:
            return []
        else:
            self.__get_buy_product_id_sql()
            return self.product_id


    def __get_product_catalog_id(self,id):
        """
        :param id: 产品id
        :return: 增加到分类ID
        """
        try:
            sql="select * from product_entity where entity_id = %s" % id
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for data in results:
                yield data['catalog_id']
        except:
            self.logger.info(u'搜索product_entity数据库出错')
            sys.exit()

    def __get_product_catalog_name(self,id):
        """
        :param id: 分类id
        :return: 分类名称
        """
        try:
            sql="select * from catalog where catalog_id = %s" % id
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for data in results:
                yield data['catalog_name']
        except:
            self.logger.info(u'搜索product_entity数据库出错')
            sys.exit()


    @property
    def BuyProductCatalogID(self):
        """
        获取客户购买的分类ID
        :return: 
        """
        if self.email_id==[]:
            return []
        else:
            self.__get_buy_product_id_sql()
            self.catalog_id=[]
            for id in self.product_id:
                list(map(lambda a:self.catalog_id.append(a),list(self.__get_product_catalog_id(id))))
            return list(set(self.catalog_id))

    @property
    def BuyProductCatalogName(self):
        """
        获取客户购买的分类名称
        :return: 
        """
        if self.email_id == []:
            return []
        else:
            self.__get_buy_product_id_sql()
            self.catalog_id = []
            for id in self.product_id:
                list(map(lambda a: self.catalog_id.append(a), list(self.__get_product_catalog_id(id))))
            self.catalog_name=[]
            for id in list(set(self.catalog_id)):
                list(map(lambda a: self.catalog_name.append(a), list(self.__get_product_catalog_name(id))))
            return self.catalog_name


    def __get_vip_sql(self,id):
        """
        :param id: 客户id
        :return: 客户vip
        """
        try:
            sql = "select * from vip where customer_id = %s" % id
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for data in results:
                yield data['vip_id']
        except:
            self.logger.info(u'搜索vip数据库出错')
            sys.exit()


    @property
    def GetVip(self):
        """
        获得vip列表
        :return: 
        """
        if self.email_id==[]:
            return []
        else:
            vip_list=[]
            for id in self.email_id:
                list(map(lambda a:vip_list.append(a),list(self.__get_vip_sql(id))))
            return vip_list

    @property
    def GetIsRegister(self):
        """
        :return: 是否注册
        """
        return self.is_register

    def __get_buy_product_time(self):
        """
        获取分类id
        :return: 
        """
        try:
            if self.email_id==[]:
                self.time=[]
            else:
                sql="select * from sales_order_product where customer_entity_id in (%s)" % ','.join(str(i) for i in self.email_id)
                self.cursor.execute(sql)
                results = self.cursor.fetchall()
                self.time = []
                for name in results:
                    self.time.append(name['order_time'])
        except:
            self.logger.info(u'搜索sales_order_product数据库出错')
            sys.exit()

    @property
    def GetLastBuyTime(self):
        """
        :return: 获得最后一单购买时间
        """
        self.__get_buy_product_time()
        if self.time==[]:
            return None
        else:
            return sorted(self.time)[-1]


    def __get_order_counon(self):
        """
        :return: Coupon code列表
        """
        try:
            if self.email_id==[]:
                self.coupon=[]
            else:
                sql="select * from sales_order where customer_entity_id in (%s)" % ','.join(str(i) for i in self.email_id)
                self.cursor.execute(sql)
                results = self.cursor.fetchall()
                self.coupon = []
                for name in results:
                    if name['coupon_code']=='':
                        pass
                    else:
                        self.coupon.append(name['coupon_code'])
        except:
            self.logger.info(u'搜索sales_order数据库出错')
            sys.exit()

    @property
    def GetIsCounpon(self):
        """
        获取是否使用过coupon
        :return: 
        """
        self.__get_order_counon()
        if self.coupon==[]:
            return False
        else:
            return True

    def __get_customer_black(self):
        """
        判断是否在黑名单中
        :return: 
        """
        try:
            if self.email_id==[]:
                return False
            else:
                sql="select * from customer_black_searon where customer_id in (%s) and statu=1" % ','.join(str(i) for i in self.email_id)
                self.cursor.execute(sql)
                results = self.cursor.fetchall()
                if results == ():
                    return False
                else:
                    return True
        except:
            self.logger.info(u'搜索customer_black_searon数据库出错')
            sys.exit()


    def __get_customer_review_star(self):
        """
        判断用户评论星级
        :return: 
        """
        try:
            if self.email_id==[]:
                return False
            else:
                sql="select * from customer_review where customer_entity_id in (%s)" % ','.join(str(i) for i in self.email_id)
                self.cursor.execute(sql)
                results = self.cursor.fetchall()
                self.star=[]
                for data in results:
                    self.star.append(data['star'])
        except:
            self.logger.info(u'搜索customer_review数据库出错')
            sys.exit()

    @property
    def GetIsBlack(self):
        """
        获得是否黑名单
        :return: 
        """
        return self.__get_customer_black()


    @property
    def GetReviewStar(self):
        """
        获得评论星级
        :return: 
        """
        self.__get_customer_review_star()
        return self.star

    def __get_data_after_buy_sum(self,day,today):
        """
        获得多少天内的购买次数
        :param day: 
        :return: 
        """
        day = (datetime.datetime.now() + datetime.timedelta(-day)).strftime("%Y-%m-%d")
        today = (datetime.datetime.now() + datetime.timedelta(-today)).strftime("%Y-%m-%d")
        try:

            if self.email_id==[]:
                return 0
            else:
                sql="select * from sales_order where customer_entity_id in ({0}) and created_at<'{1}' and created_at >'{2}'".format(','.join(str(i) for i in self.email_id),today,day)
                self.cursor.execute(sql)
                results = self.cursor.fetchall()
                return len(results)
        except:
            self.logger.info(u'搜索customer_review数据库出错')
            sys.exit()

    def GetBuySum(self,day,today):
        """
        获得购买次数
        :param day: 大于day
        :param today: 小于today
        :return: 
        """
        return self.__get_data_after_buy_sum(day,today)

    def __get_all_data_after_buy_sum(self):
        """
        获得总的购买次数
        :param day: 
        :return: 
        """
        try:

            if self.email_id==[]:
                return 0
            else:
                sql="select * from sales_order where customer_entity_id in ({0})".format(','.join(str(i) for i in self.email_id))
                self.cursor.execute(sql)
                results = self.cursor.fetchall()
                return len(results)
        except:
            self.logger.info(u'搜索customer_review数据库出错')
            sys.exit()

    @property
    def GetAllBuySum(self):
        """
        获得总购买次数
        :return: 
        """
        return self.__get_all_data_after_buy_sum()



if __name__=="__main__":
    GetUserInfo=GetUserInfo()
    GetUserInfo.Email('info@surfclubcoffsharbour.com')
    print(GetUserInfo.EmailID)#获得邮箱id
    print(GetUserInfo.BuyProductID)#获得购买产品id
    print(GetUserInfo.BuyProductCatalogID)#获得购买产品分类id
    print(GetUserInfo.BuyProductCatalogName)#获得购买产品分类名称
    print(GetUserInfo.GetVip)#获得vip级别
    print(GetUserInfo.GetIsRegister)#获得是否注册
    print(GetUserInfo.GetLastBuyTime)#获得购买最后时间
    print(GetUserInfo.GetIsCounpon)#获得是否使用coupon
    print(GetUserInfo.GetIsBlack)#获得是否黑名单
    print(GetUserInfo.GetReviewStar)#获得评论星级
    print(GetUserInfo.GetBuySum(100,0))#获得多少天内的购买次数