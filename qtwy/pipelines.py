import uuid
import re
from datetime import datetime

import pymysql

from qtwy.items import QtwyJobInfoItem, ProcessedQtwyJobInfoItem


# 给Item添加major(学科关键字)的值
class ItemMajorPipeline:
    def __init__(self, JOB_MAJOR):
        self.JOB_MAJOR = JOB_MAJOR

    @classmethod
    def from_settings(cls, settings):
        return cls(settings['JOB_MAJOR'])

    def process_item(self, item, spider):
        item['major'] = self.JOB_MAJOR
        return item


# 处理　RawData(原始数据) -> ProcessedData(真实加工后的数据)
class ProcessRawDataPipeline:
    def __init__(self):
        self.p_item = ProcessedQtwyJobInfoItem()

    def process_item(self, item: QtwyJobInfoItem, spider) -> ProcessedQtwyJobInfoItem:
        # 实现 RawData(原始数据) -> ProcessedData(真实加工后的数据)
        # 处理 15 + 24(welfare未处理) + 3(空) = 42 个字段
        # 并且返回 ProcessedData
        self.process_blank_property()
        self.process_salary(item['salary'])
        self.process_position_description(item['position_description'])
        self.process_welfare(item['welfare'])
        self.clone_raw_val(item)
        return self.p_item

    def process_salary(self, salary):
        # 处理 薪酬 (2)

        # 将 %(min)d-%(max)d %s/%s ->  min, max
        pattern = r'(?P<min_salary>\d*?.\d*?|\d*?)-(?P<max_salary>\d*?.\d*?|\d*?)(?P<dw1>千|万)/(?P<dw2>月|年)'
        match = re.match(pattern, salary)
        # 入库单位 千/月
        min_salary = float(match.group('min_salary'))
        max_salary = float(match.group('max_salary'))

        if match.group('dw1').__eq__('万'):
            min_salary *= 10
            max_salary *= 10
        if match.group('dw2').__eq__('年'):
            # 以一年 30 * 12 = {360} 天来算
            min_salary /= 12
            max_salary /= 12

        self.p_item['min_salary'] = match['min_salary']
        self.p_item['max_salary'] = match['max_salary']

    def process_position_description(self, pd):
        # 处理 职位简介 (6)
        d_list = eval(pd)

        # 处理 company信息 (2)
        m1 = re.match(r'((?P<city1>\w{2,})-(?P<area1>\w{2,}))|(?P<city2>\w{2,})',
                      d_list[0])
        if m1.group('city2') is not None:
            self.p_item['company_location_city'] = m1.group('city2')
            self.p_item['company_location_area'] = 'null'
        else:
            self.p_item['company_location_city'] = m1.group('city1')
            self.p_item['company_location_area'] = m1.group('area1')

        # 处理 工作经验 experience (2)
        m2 = re.match(r'(((?P<min1>\d*)-(?P<max1>\d*))|(?P<min2>\d*))(年经验)', d_list[1])
        if m2:
            if m2.group('min2') is not None:
                self.p_item['limit_job_min_experience'] = m2.group('min2')
                self.p_item['limit_job_max_experience'] = -1
            else:
                self.p_item['limit_job_min_experience'] = m2.group('min1')
                self.p_item['limit_job_max_experience'] = m2.group('max1')
        else:
            m2 = re.match(r'(?P<flag>应届生/在校生)')
            if m2:
                self.p_item['limit_is_zxs'] = 1
                self.p_item['limit_is_yjs'] = 1

        # 处理 学历 (1)
        m3 = re.match(r'(?P<xl>\w*)', d_list[2])
        if m3:
            self.p_item['limit_job_education'] = m3.group('xl')

        # 处理 招聘人数 (1)
        m4 = re.match(r'(招(?P<n>\d*)人)', d_list[3])
        if m4 and m4.group('n') is not None:
            self.p_item['number_of_recruitment'] = m4.group('n')
        else:
            self.p_item['number_of_recruitment'] = -1

    def process_welfare(self, welfare):
        # 处理 welfare 字段 (24)

        # 全部处理为 0 先
        self.p_item.setdefault('welfare_wxyj', 0)
        self.p_item.setdefault('welfare_bcyb', 0)
        self.p_item.setdefault('welfare_bcgjj', 0)
        self.p_item.setdefault('welfare_cb', 0)
        self.p_item.setdefault('welfare_mfbc', 0)
        self.p_item.setdefault('welfare_jtbt', 0)
        self.p_item.setdefault('welfare_txbt', 0)
        self.p_item.setdefault('welfare_gwbt', 0)
        self.p_item.setdefault('welfare_jbbt', 0)
        self.p_item.setdefault('welfare_zfbt', 0)
        self.p_item.setdefault('welfare_bss', 0)
        self.p_item.setdefault('welfare_dqjc', 0)
        self.p_item.setdefault('welfare_px', 0)
        self.p_item.setdefault('welfare_jxj', 0)
        self.p_item.setdefault('welfare_xmtc', 0)
        self.p_item.setdefault('welfare_nzj', 0)
        self.p_item.setdefault('welfare_qqj', 0)
        self.p_item.setdefault('welfare_dx', 0)
        self.p_item.setdefault('welfare_sx', 0)
        self.p_item.setdefault('welfare_txgzz', 0)
        self.p_item.setdefault('welfare_dxnj', 0)
        self.p_item.setdefault('welfare_ly', 0)
        self.p_item.setdefault('welfare_srfl', 0)
        self.p_item.setdefault('welfare_jrfl', 0)

    def clone_raw_val(self, item):
        # clone 公共字段 (7)
        data = {
            'major': item['major'],
            'source_url': item['source_url'],
            'position_name': item['position_name'],
            # 改名项
            'company_enterprise_name': item['enterprise_name'],
            'position_detail_description': item['position_detail_description'],
            'contact_info': item['contact_info'],
            'company_info': item['company_info'],
        }
        for (k, v) in data.items():
            self.p_item[k] = v

    def process_blank_property(self):
        # 处理空字段 3 个
        self.p_item['limit_is_zxs'] = 0
        self.p_item['limit_is_yjs'] = 0
        self.p_item['publish_time'] = 0


# RawData 入库
class RawDataMySQLPipeline:
    def __init__(self, config):
        self.config = config

    @classmethod
    def from_settings(cls, settings):
        return cls(settings['MYSQL_CONFIGURATION'])

    def open_spider(self, spider):
        # 建立数据库连接
        self.connection = pymysql.connect(**self.config)
        # 从连接中
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item: QtwyJobInfoItem, spider) -> QtwyJobInfoItem:
        # if not isinstance(item, QtwyJobInfoItem):
        #     raise Exception()
        item['id'] = str(uuid.uuid1())
        item['createtime'] = int(datetime.now().timestamp())
        item['modifiedtime'] = int(datetime.now().timestamp())
        self.insertInToSQL(dict(item))
        return item

    def insertInToSQL(self, data: dict):
        if len(data) != 13:
            raise Exception()
        sql = """
        insert into 
        `jms_raw_job_item`
        set 
        `id` = %(id)s,
        `source_url` = %(source_url)s,
        `major` = %(major)s,
        `position_name` = %(position_name)s,
        `salary` = %(salary)s,
        `enterprise_name` = %(enterprise_name)s,
        `position_description` = %(position_description)s,
        `welfare` = %(welfare)s,
        `position_detail_description` = %(position_detail_description)s,
        `contact_info` = %(contact_info)s,
        `company_info` = %(company_info)s,
        `createtime` = %(createtime)s,
        `modifiedtime` = %(modifiedtime)s
        """
        self.cursor.execute(sql, data)
        self.connection.commit()


# ProcessedData 入库
class ProcessedDataMySQLPipeline:
    def __init__(self, config):
        self.config = config

    @classmethod
    def from_settings(cls, settings):
        return cls(settings['MYSQL_CONFIGURATION'])

    def open_spider(self, spider):
        # 建立数据库连接
        self.connection = pymysql.connect(**self.config)
        # 从连接中
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        if not isinstance(item, ProcessedQtwyJobInfoItem):
            raise Exception()
        item['id'] = str(uuid.uuid1())
        item['createtime'] = int(datetime.now().timestamp())
        item['modifiedtime'] = int(datetime.now().timestamp())
        self.insertInToSQL(dict(item))
        return item

    def insertInToSQL(self, data: dict):
        if len(data) != 45:
            raise Exception()
        sql = """
        insert into 
        `jms_job_item`
        set 
        `id` = %(id)s,


        `source_url` = %(source_url)s,
        `major` = %(major)s,
        `position_name` = %(position_name)s,


        `min_salary` = %(min_salary)s,
        `max_salary` = %(max_salary)s,


        `company_enterprise_name` = %(company_enterprise_name)s,
        `company_location_city` = %(company_location_city)s,
        `company_location_area` = %(company_location_area)s,


        `limit_job_min_experience` = %(limit_job_min_experience)s,
        `limit_job_max_experience` = %(limit_job_max_experience)s,
        `limit_job_education` = %(limit_job_education)s,
        `limit_is_zxs` = %(limit_is_zxs)s,
        `limit_is_yjs` = %(limit_is_yjs)s,


        `number_of_recruitment` = %(number_of_recruitment)s,
        `publish_time` = %(publish_time)s,


        `welfare_wxyj` = %(welfare_wxyj)s,
        `welfare_bcyb` = %(welfare_bcyb)s,
        `welfare_bcgjj` = %(welfare_bcgjj)s,
        `welfare_cb` = %(welfare_cb)s,
        `welfare_mfbc` = %(welfare_mfbc)s,
        `welfare_jtbt` = %(welfare_jtbt)s,
        `welfare_txbt` = %(welfare_txbt)s,
        `welfare_gwbt` = %(welfare_gwbt)s,
        `welfare_jbbt` = %(welfare_jbbt)s,
        `welfare_zfbt` = %(welfare_zfbt)s,
        `welfare_bss` = %(welfare_bss)s,
        `welfare_dqjc` = %(welfare_dqjc)s,
        `welfare_px` = %(welfare_px)s,
        `welfare_jxj` = %(welfare_jxj)s,
        `welfare_xmtc` = %(welfare_xmtc)s,
        `welfare_nzj` = %(welfare_nzj)s,
        `welfare_qqj` = %(welfare_qqj)s,
        `welfare_dx` = %(welfare_dx)s,
        `welfare_sx` = %(welfare_sx)s,
        `welfare_txgzz` = %(welfare_txgzz)s,
        `welfare_dxnj` = %(welfare_dxnj)s,
        `welfare_ly` = %(welfare_ly)s,
        `welfare_srfl` = %(welfare_srfl)s,
        `welfare_jrfl` = %(welfare_jrfl)s,


        `position_detail_description` = %(position_detail_description)s,
        `contact_info` = %(contact_info)s,
        `company_info` = %(company_info)s,


        `createtime` = %(createtime)s,
        `modifiedtime` = %(modifiedtime)s
        """
        self.cursor.execute(sql, data)
        self.connection.commit()
