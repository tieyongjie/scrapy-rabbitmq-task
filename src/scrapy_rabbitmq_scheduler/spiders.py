# -*- coding: utf-8 -*-
import scrapy
import pickle
from scrapy.utils.request import request_from_dict


class RabbitSpider(scrapy.Spider):
    def _make_request(self, mframe, hframe, body):
        try:
            # 反序列化 body 数据
            data = pickle.loads(body)
            # 获取请求的 URL 和其他参数
            url = data.get('url')
            method = data.get('method', 'GET').upper()  # 默认 GET，如果是 POST 需要设置为 'POST'
            headers = data.get('headers', {})
            cookies = data.get('cookies', {})
            body_data = data.get('body')  # 可能是 POST 请求的表单数据
            callback_str = data.get('callback')  # 回调函数名称（字符串）
            errback_str = data.get('errback')  # 错误回调函数名称（字符串）
            meta = data.get('meta', {})
            # 尝试从全局字典中获取回调函数
            # 使用爬虫实例的 `getattr` 方法获取回调函数
            callback = getattr(self, callback_str, None) if callback_str else None
            errback = getattr(self, errback_str, None) if errback_str else None
            # # 确保回调函数存在
            # if callback is None:
            #     self.logger.error(f"Callback function '{callback_str}' not found.")
            # if errback is None:
            #     self.logger.error(f"Errback function '{errback_str}' not found.")
            # 判断请求方法，如果是 POST，则使用 FormRequest
            if callback:
                if method == 'POST':
                    # FormRequest 适用于带有表单数据的 POST 请求
                    request = scrapy.FormRequest(
                        url=url,
                        method='POST',
                        headers=headers,
                        cookies=cookies,
                        body=body_data,  # 请求的主体
                        callback=callback,
                        errback=errback,
                        meta=meta,
                        dont_filter=True
                    )
                else:
                    # 默认处理 GET 请求
                    request = scrapy.Request(
                        url=url,
                        headers=headers,
                        cookies=cookies,
                        callback=callback,
                        errback=errback,
                        meta=meta,
                        dont_filter=True
                    )
            else: pass
        except Exception as e:
            body = body.decode()
            request = scrapy.Request(body, callback=self.parse, dont_filter=True)
        return request
