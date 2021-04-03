import uuid
from datetime import datetime

import pymysql

from qtwy.items import QtwyJobInfoItem


# 给Item添加值
class ItemMajorPipeline:
    def __init__(self, JOB_MAJOR):
        self.JOB_MAJOR = JOB_MAJOR

    @classmethod
    def from_settings(cls, settings):
        return cls(settings['JOB_MAJOR'])

    def process_item(self, item, spider):
        item['major'] = self.JOB_MAJOR
        return item


class MySQLPipeline:
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
        if not isinstance(item, QtwyJobInfoItem):
            raise Exception()
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
