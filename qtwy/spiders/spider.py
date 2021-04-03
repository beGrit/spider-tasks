import scrapy
from scrapy.loader import ItemLoader
from w3lib.html import remove_tags

from qtwy.items import QtwyJobInfoItem


class QTWYSpider(scrapy.Spider):
    name = 'qtwy_job'

    pages = (x for x in range(2, 7))

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }

    cookies = {
        'guid': 'f44bcaeec733dbdcc6d7f3c7fd4b22c8',
        'nsearch': 'jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D	',
        'search': 'jobarea%7E%60000000%7C%21ord_field%7E%600%7C%21recentSearch0%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%C8%ED%BC%FE%B9%A4%B3%CC%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch1%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA32%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%C8%ED%BC%FE%B9%A4%B3%CC%CA%A6%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch2%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%C8%ED%BC%FE%B9%A4%B3%CC%CA%A6%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch3%7E%60360000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A…	',
    }

    query_parameters = {
        'lang': 'c',
        'postchannel': '0000',
        'wordyear': '99',
        'cotype': '99',
        'degreefrom': '99',
        'jobterm': '99',
        'companysize': '99',
        'ord_field': '0',
        'dibiaoid': '0',
        'line': '',
        'welfare': '',
    }

    def start_requests(self):
        url = 'https://search.51job.com/list/000000,000000,0000,00,9,99,%25E8%25BD%25AF%25E4%25BB%25B6%25E5%25B7%25A5%25E7%25A8%258B,2, 1.html'
        yield scrapy.Request(url, headers=self.headers, cookies=self.cookies, callback=self.parse)

    def parse(self, response, **kwargs):
        import json
        data = json.loads(response.text)
        for post in data.get('engine_search_result', []):
            item_loader = ItemLoader(item=QtwyJobInfoItem(), response=response)
            item_loader.add_value('position_name', post['job_name'])
            item_loader.add_value('salary', post['providesalary_text'])
            item_loader.add_value('enterprise_name', post['company_name'])
            item_loader.add_value('position_description', str(post['attribute_text']))
            item_loader.add_value('source_url', post['job_href'])
            yield scrapy.Request(post['job_href'], callback=self.parse_detail, meta={'item_loader': item_loader})
        page = next(self.pages)
        url = 'https://search.51job.com/list/000000,000000,0000,00,9,99,%25E8%25BD%25AF%25E4%25BB%25B6%25E5%25B7%25A5%25E7%25A8%258B,2, ' + str(
            page) + '.html'
        yield scrapy.Request(url, headers=self.headers, cookies=self.cookies, callback=self.parse)

    def parse_detail(self, response, **kwargs):
        item_loader = response.meta['item_loader']
        item_loader.response = response
        root_selector = response.selector
        header_info = root_selector.xpath('.//div[@class="tHeader tHjob"]')
        main_info = root_selector.xpath('.//div[@class="tCompany_main"]')
        data = {
            'welfare': remove_tags(header_info.xpath('.//div[@class="jtag"]').get()),
            'position_detail_description': remove_tags(main_info.xpath('.//div[@class="bmsg job_msg inbox"]').get()),
            'contact_info': remove_tags(
                main_info.xpath('.//span[text()="联系方式"]/parent::h2/following-sibling::div').get()),
            'company_info': remove_tags(
                main_info.xpath('.//span[text()="公司信息"]/parent::h2/following-sibling::div').get()),
        }
        item = item_loader.load_item()
        item.collect(**data)
        print(list(item))
        yield item
