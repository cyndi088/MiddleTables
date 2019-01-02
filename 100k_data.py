# -*- coding: utf-8 -*-
import time
from datetime import datetime
import pymysql
from pymongo import MongoClient
from dateutil import parser


class MiddleTable(object):

    def __init__(self):
        self.mysql_host = "192.168.10.121"
        self.mysql_user = 'hzyg'
        self.mysql_password = '@hzyq20180426..'
        self.MONGO_HOST = "localhost"
        self.MONGO_PORT = 27017

    def open_sql(self, ms_db, mo_db, mo_coll1, mo_coll2):
        self.link = pymysql.connect(self.mysql_host, self.mysql_user, self.mysql_password, ms_db)
        self.link.set_charset('utf8')
        self.cursor = self.link.cursor()
        self.client = MongoClient(host=self.MONGO_HOST, port=self.MONGO_PORT)
        self.mo_db = self.client[mo_db]
        self.coll1 = self.mo_db[mo_coll1]
        self.coll2 = self.mo_db[mo_coll2]

    def input_sql(self):
        data_list = self.coll1.find(no_cursor_timeout=True)
        i = 1
        for data in data_list:
            if data['next'] == '':
                print(i)
                print(data)
                item = {}
                item["stampDateTime"] = datetime.now()
                item["commodityName"] = self.food_name(data["commodityName"])
                item["corpNameBy"] = data["corpNameBy"]
                item["addressBy"] = data["addressBy"]
                item["addressByRegionId"] = data['addressByRegionId']
                item["corpName"] = data["corpName"]
                item["address"] = data["address"]
                item["addressRegionId"] = self.zoning(data["address"])
                item["createDate"] = self.time_format(data["createDate"])
                item["flId"] = self.fl(data["fl"])
                item["ggh"] = data["ggh"]
                item["ggrq"] = data["ggrq"]
                item["rwly_id"] = self.rwly(data["rwly"])
                item["model"] = data["model"]
                item["newsDetailTypeId"] = 1
                item["note"] = "/"
                item["productionDate"] = "/"
                item["trademark"] = "/"
                item["unqualifiedItem"] = "/"
                item["checkResult"] = "/"
                item["standardValue"] = "/"
                item["batchNumber"] = "/"
                self.coll2.save(item)
                i += 1
                data['next'] = i
                self.coll1.save(data)
        self.coll1.update_many({}, {'$set': {'next': ''}})
        data_list.close()

    def close_sql(self):
        self.link.close()

    @staticmethod
    def zoning(str):
        try:
            if str == None or str == "/":
                return 0
            else:
                mysql_host = "192.168.10.121"
                mysql_user = 'hzyg'
                mysql_password = '@hzyq20180426..'
                msyql_db = 'yfhunt'
                link = pymysql.connect(mysql_host, mysql_user, mysql_password, msyql_db)
                link.set_charset('utf8')
                cursor = link.cursor()
                sql1 = "select region_name from region r where r.parent_id in (select r.region_id from region r where r.parent_id=(select r.region_id from region r where r.region_name='浙江省'))"
                cursor.execute(sql1)
                allData1 = cursor.fetchall()
                sql2 = "select region_name from region r where r.parent_id=(select r.region_id from region r where r.region_name='浙江省')"
                cursor.execute(sql2)
                allData2 = cursor.fetchall()
                i = 0
                stp = []
                while i < len(allData1):
                    j = 0
                    while j < len(allData1[i]):
                        if allData1[i][j][:-1] in str:
                            stp.append(allData1[i][j])
                            sql3 = "select region_id from region r where r.region_name='%s'" % stp[0]
                            cursor.execute(sql3)
                            allData3 = cursor.fetchall()
                            region_id = int(allData3[0][0])
                            return region_id
                            break
                        else:
                            j += 1
                    i += 1
                if not stp:
                    m = 0
                    while m < len(allData2):
                        n = 0
                        while n < len(allData2[m]):
                            if allData2[m][n][:-1] in str:
                                stp.append(allData2[m][n])
                                sql4 = "select region_id from region r where r.region_name='%s'" % stp[0]
                                cursor.execute(sql4)
                                allData4 = cursor.fetchall()
                                region_id = int(allData4[0][0])
                                return region_id
                                break
                            else:
                                n += 1
                        m += 1
                if not stp and "浙江" in str:
                    return 12
                elif not stp and "浙江" not in str:
                    return 1
        except TypeError as e:
            return 0

    @staticmethod
    def str_time(str):
        return parser.parse(str)

    @staticmethod
    def rwly(num):
        if num == 1:
            return 520
        elif num == 2:
            return 521
        elif num == 3:
            return 522
        elif num == 4:
            return 523
        else:
            return 524

    @staticmethod
    def fl(name):
        if name:
            mysql_host = "192.168.10.121"
            mysql_user = 'hzyg'
            mysql_password = '@hzyq20180426..'
            msyql_db = 'yfhunt'
            link = pymysql.connect(mysql_host, mysql_user, mysql_password, msyql_db)
            link.set_charset('utf8')
            cursor = link.cursor()
            sql = "select sys_data_group_id from sys_data_item where key_value='%s'" % name
            cursor.execute(sql)
            allData = cursor.fetchone()
            if allData:
                return allData[0]
            else:
                return 82
        else:
            return 82

    @staticmethod
    def time_format(str):
        if isinstance(str, datetime):
            return str
        elif isinstance(str, int):
            return '/'
        elif isinstance(str, float):
            return '/'
        else:
            if str[:2] == "20":
                if "-" in str:
                    try:
                        stp = str[:10]
                        ss1 = parser.parse(stp)
                        return ss1
                    except:
                        return datetime.now()
                elif "." in str:
                    ls = str.split(".")
                    if len(ls) == 3:
                        try:
                            stp = time.strptime(str, '%Y.%m.%d')
                            ss2 = parser.parse(time.strftime("%Y-%m-%d", stp))
                            return ss2
                        except:
                            return datetime.now()
                    elif len(ls) == 2:
                        str = ls[0]
                        if len(str) >= 8:
                            stp = str[:4] + "-" + str[4:6] + "-" + str[6:8]
                            try:
                                ss3 = parser.parse(stp)
                                return ss3
                            except:
                                return datetime.now()
                        else:
                            return datetime.now()
                    else:
                        return datetime.now()
                elif "/" in str:
                    ls = str.split("/")[:3]
                    if len(ls) >= 3:
                        try:
                            stp = ls[0] + "-" + ls[1] + "-" + ls[2][:2]
                            ss4 = parser.parse(stp)
                            return ss4
                        except:
                            return datetime.now()
                    else:
                        return datetime.now()
                else:
                    return datetime.now()
            else:
                return datetime.now()

    @staticmethod
    def food_name(name):
        new_name = name.replace('"', '')
        return new_name


if __name__ == '__main__':
    mt = MiddleTable()
    mt.open_sql('yfhunt', 'zhejiang', '200k_300k', '200k_300k_test')
    mt.input_sql()
    mt.close_sql()