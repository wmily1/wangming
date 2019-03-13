# conding=GBK
# -*- coding: utf-8 -*
import configparser
import logging,os,sys
import time
import MySQLdb
from MySQLdb import escape_string
import MySQLdb.cursors
os.chdir(sys.path[0])

class get_config(object):
    def __init__(self):
        self.log_file()
        conf = configparser.ConfigParser()
        conf.read('config.ini')
        #链接data_collection数据库
        self.__host = conf.get('database', 'host')
        self.__user = conf.get('database', 'user')
        self.__passwd = conf.get('database', 'passwd')
        self.__db = conf.get('database', 'db')
        self.__charset = conf.get('database', 'charset')
        self.db,self.cursor=self.get_database()
        #链接data_warehouse数据库
        self.__host = conf.get('database1', 'host')
        self.__user = conf.get('database1', 'user')
        self.__passwd = conf.get('database1', 'passwd')
        self.__db = conf.get('database1', 'db')
        self.__charset = conf.get('database1', 'charset')
        self.db_warehouse, self.cursor_warehouse = self.get_database()



    # 写入日志文件
    def log_file(self):
        """
        获取日志文件
        :return: 返回日志文件的self程序名
        """
        logging.basicConfig(level=logging.INFO,
                            filename='log.log',
                            filemode='a',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        self.logger = logging.getLogger('log.log')
        format_str = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        self.logger.addHandler(sh)

    #链接数据库
    def get_database(self):
        db = MySQLdb.connect(host=self.__host,
                             user=self.__user,
                             passwd=self.__passwd,
                             db=self.__db,
                             charset=self.__charset,
                             cursorclass=MySQLdb.cursors.DictCursor)
        cursor = db.cursor()
        self.logger.info("Connect %s Success!" % self.__db)
        return db, cursor

if __name__=='__main__':
    get_config()

