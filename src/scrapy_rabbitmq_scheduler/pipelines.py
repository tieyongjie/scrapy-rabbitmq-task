
from . import connection
from twisted.internet.threads import deferToThread
from scrapy.utils.serialize import ScrapyJSONEncoder

EXCHANGE_NAME = ''

import logging
logger = logging.getLogger()
import time

class RabbitMQPipeline(object):
    """Pushes serialized item into a RabbitMQ list/queue"""

    def __init__(self, item_key, exchange_name,connection_url):
        self.item_key = item_key
        self.exchange_name = exchange_name
        self.encoder = ScrapyJSONEncoder()
        self.connection_url= connection_url


    @classmethod
    def from_settings(cls,item_key, settings):
        exchange_name = settings.get('RABBITMQ_EXCHANGE_NAME', EXCHANGE_NAME)
        connection_url = settings.get('RABBITMQ_CONNECTION_PARAMETERS')
        return cls(item_key, exchange_name,connection_url)

    @classmethod
    def from_crawler(cls, crawler):
        if hasattr(crawler.spider, 'items_key'):
            item_key = crawler.spider.items_key
        else:
            item_key = 'items_{spider_name}'.format(
                spider_name=crawler.spider.name)
        return cls.from_settings(item_key,crawler.settings)

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        key = self.item_key
        data = self.encoder.encode(item)
        try_time = 1
        while try_time<10:
            try:
                self.server = connection.connect(self.connection_url)
                self.channel = connection.get_channel(self.server, self.item_key)
                self.channel.basic_publish(exchange=self.exchange_name,
                                        routing_key=key,
                                        body=data)
                return item
            except Exception as e:
                logger.exception(e)
                logger.error('process item failed! try_time:{}'.format(try_time))
                try_time += 1
                time.sleep(1)
                self.channel = connection.get_channel(self.server, self.item_key)
        return item