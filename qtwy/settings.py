BOT_NAME = 'qtwy'

SPIDER_MODULES = ['qtwy.spiders']
NEWSPIDER_MODULE = 'qtwy.spiders'

ROBOTSTXT_OBEY = True

ITEM_PIPELINES = {
    'qtwy.pipelines.ItemMajorPipeline': 100,
    'qtwy.pipelines.RawDataMySQLPipeline': 200,
    'qtwy.pipelines.ProcessRawDataPipeline': 300,
    'qtwy.pipelines.ProcessedDataMySQLPipeline': 400,
}

MYSQL_CONFIGURATION = {
    'host': '118.31.15.23',
    'port': 3306,
    'user': 'lsf',
    'password': 'LSFlsf123',
    'database': 'job_info_db2',
}

DOWNLOAD_DELAY = 1  # 间隔时间
CONCURRENT_REQUESTS = 5  # 请求并发数

JOB_MAJOR = '软件工程'
