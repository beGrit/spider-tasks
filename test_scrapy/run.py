from scrapy import cmdline


def crawl(spider_name: str):
    cmdline.execute(('scrapy crawl %s' % spider_name).split())


if __name__ == '__main__':
    crawl('qtwy_job')
