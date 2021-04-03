import scrapy
from itemadapter import ItemAdapter
from itemloaders.processors import MapCompose, TakeFirst
from w3lib.html import remove_tags


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
