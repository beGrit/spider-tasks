from dataclasses import dataclass, field
from typing import Optional

import scrapy
from itemadapter import ItemAdapter
from itemloaders.processors import MapCompose, TakeFirst
from w3lib.html import remove_tags

"""
    QtwyJobInfoItem
    字段数: 45个
"""


# 13
class QtwyJobInfoItem(scrapy.Item):
    id = scrapy.Field()
    createtime = scrapy.Field()
    modifiedtime = scrapy.Field()
    source_url = scrapy.Field(output_processor=TakeFirst())
    major = scrapy.Field()
    position_name = scrapy.Field(output_processor=TakeFirst())
    salary = scrapy.Field(output_processor=TakeFirst())
    enterprise_name = scrapy.Field(output_processor=TakeFirst())
    position_description = scrapy.Field(output_processor=TakeFirst())
    welfare = scrapy.Field(output_processor=TakeFirst())
    position_detail_description = scrapy.Field()
    contact_info = scrapy.Field(input_processor=MapCompose(remove_tags))
    company_info = scrapy.Field()

    def collect(self, **kwargs):
        adapter = ItemAdapter(self)
        for (k, v) in kwargs.items():
            adapter[k] = v


"""
    ProcessedQtwyJobInfoItem
    字段数: 45个
"""


# 45
class ProcessedQtwyJobInfoItem(scrapy.Item):
    id = scrapy.Field()
    # 1
    source_url = scrapy.Field()
    major = scrapy.Field()
    position_name = scrapy.Field()
    # 3
    min_salary = scrapy.Field()
    max_salary = scrapy.Field()
    # 2
    company_enterprise_name = scrapy.Field()
    company_location_city = scrapy.Field()
    company_location_area = scrapy.Field()
    # 3
    limit_job_min_experience = scrapy.Field()
    limit_job_max_experience = scrapy.Field()
    limit_job_education = scrapy.Field()
    limit_is_zxs = scrapy.Field()
    limit_is_yjs = scrapy.Field()
    number_of_recruitment = scrapy.Field()
    publish_time = scrapy.Field()
    # 7
    welfare_wxyj = scrapy.Field()
    welfare_bcyb = scrapy.Field()
    welfare_bcgjj = scrapy.Field()
    welfare_cb = scrapy.Field()
    welfare_mfbc = scrapy.Field()
    welfare_jtbt = scrapy.Field()
    welfare_txbt = scrapy.Field()
    welfare_gwbt = scrapy.Field()
    welfare_jbbt = scrapy.Field()
    welfare_zfbt = scrapy.Field()
    welfare_bss = scrapy.Field()
    welfare_dqjc = scrapy.Field()
    welfare_px = scrapy.Field()
    welfare_jxj = scrapy.Field()
    welfare_xmtc = scrapy.Field()
    welfare_nzj = scrapy.Field()
    welfare_qqj = scrapy.Field()
    welfare_dx = scrapy.Field()
    welfare_sx = scrapy.Field()
    welfare_txgzz = scrapy.Field()
    welfare_dxnj = scrapy.Field()
    welfare_ly = scrapy.Field()
    welfare_srfl = scrapy.Field()
    welfare_jrfl = scrapy.Field()
    # 24
    position_detail_description = scrapy.Field()
    contact_info = scrapy.Field()
    company_info = scrapy.Field()
    # 3
    createtime = scrapy.Field()
    modifiedtime = scrapy.Field()

    # 2

    def collect(self, **kwargs):
        adapter = ItemAdapter(self)
        for (k, v) in kwargs.items():
            adapter[k] = v


# @dataclass
# class Item:
#     name: Optional[str, float] = field(default='')
#     pass
