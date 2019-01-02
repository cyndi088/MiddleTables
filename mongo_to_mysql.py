# -*- coding: utf-8 -*-
import pymysql
from pymongo import MongoClient


class MiddleTable(object):

    def __init__(self):
        self.mysql_host = "192.168.10.121"
        self.mysql_user = 'hzyg'
        self.mysql_password = '@hzyq20180426..'
        self.MONGO_HOST = '127.0.0.1'
        self.MONGO_PORT = 27017
        # self.MONGO_USER = ''
        # self.PSW = ''

    def open_sql(self, ms_db, mo_db, mo_coll):
        self.link = pymysql.connect(self.mysql_host, self.mysql_user, self.mysql_password, ms_db)
        self.link.set_charset('utf8')
        self.cursor = self.link.cursor()
        self.client = MongoClient(host=self.MONGO_HOST, port=self.MONGO_PORT)
        self.mo_db = self.client[mo_db]
        self.coll = self.mo_db[mo_coll]

    def input_sql(self):
        producer_list = self.coll.distinct('corpName', {})
        seller_list = self.coll.distinct('corpNameBy', {})
        for name in producer_list or seller_list:
            if name != '/':
                detail_list = self.coll.find({'$or': [{'corpName': name, 'corpNameBy': name}]})
                for detail in detail_list:
                    inspection_id = detail['_id']
                    produce_name = detail['corpName']
                    if produce_name != '/':
                        sql = "select id from sys_organization where name='%s'" % produce_name
                        self.cursor.execute(sql)
                        produce_id = self.cursor.fetchone()
                        if not produce_id:
                            break
                        else:
                            produce_id = produce_id[0]
                    else:
                        produce_id = None
                    seller_name = detail['corpNameBy']
                    if seller_name != '/':
                        sql = "select id from sys_organization where name='%s'" % seller_name
                        self.cursor.execute(sql)
                        seller_id = self.cursor.fetchone()
                        if not seller_id:
                            break
                        else:
                            seller_id = seller_id[0]
                            sql = "select supervise_id from sys_organization_ascription where organization_id='%s'" % seller_id
                            self.cursor.execute(sql)
                            supervise_id = self.cursor.fetchone()
                            supervise_id = supervise_id[0]
                    else:
                        seller_id = None
                        supervise_id = None
                    security_results = detail['newsDetailType']
                    if security_results >= 54 and security_results <= 76 or security_results == 100:
                        security_results = 1
                    elif security_results >= 77 and security_results <= 99 or security_results ==101:
                        security_results = 2
                    data_type = detail['rwly']
                    if '省抽' in data_type:
                        data_type = 521
                    elif '国抽' in data_type:
                        data_type = 520
                    else:
                        data_type = 526
                    status = detail['status']
                    notice_date = detail['ggrq']
                    sql = """INSERT INTO organization_inspection_relation(inspection_id, producer_id, seller_id, security_results, source, data_type, status, notice_date) VALUES("%s","%d", "%d", "%s", "%d", "%d", "%d", "%s")"""  % (inspection_id, produce_id, seller_id, security_results, supervise_id, data_type, status, notice_date)
                    self.cursor.execute(sql)
                    self.link.commit()

    def close_sql(self):
        self.link.close()


mt = MiddleTable()
mt.open_sql('yfhunt', 'zhejiang', 'sheng')
mt.input_sql()
mt.close_sql()



