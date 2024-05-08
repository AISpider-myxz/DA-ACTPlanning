import copy
import datetime
import json
from urllib.parse import urlencode

import requests
import scrapy
from scrapy import Selector
from scrapy.http import Response
import time
from datetime import date, datetime, timedelta
from AISpider.items.act_items import ACTItem


class ACTSpider(scrapy.Spider):
    name = "act"
    allowed_domains = ["www.planning.act.gov.au"]
    start_urls = [
        "https://services1.arcgis.com/E5n4f1VY84i0xSjy/arcgis/rest/services/ACTGOV_DAFINDER_LIST_VIEW/FeatureServer/0/query"]

    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            #     "AISpider.middlewares.AispiderDownloaderMiddleware": None,
            #     "AISpider.middlewares.middlewares.RandomUserAgentMiddleware": 543
            "AISpider.middlewares.SeleniumMiddleware": 1,
            "AISpider.middlewares.RandomUserAgentMiddleware": 200

        },
        "ITEM_PIPELINES": {
            "AISpider.pipelines.FieldsPipline": 200,
            "AISpider.pipelines.MysqlScrapyPipeline": 300,
            # "AISpider.pipelines.MongodbPipline":299
            # 'scrapy_redis.pipelines.RedisPipeline': 300
        },
        'DOWNLOAD_DELAY': 3,
        # 'RANDOMIZE_DOWNLOAD_DELAY': True,
        'LOG_STDOUT': True,
        #'LOG_FILE': 'scrapy_act.log',
        'DOWNLOAD_TIMEOUT': 600
    }

    def __init__(self, limit=25, order_by='DA_NUMBER DESC', *args, **kwargs):
        super(ACTSpider, self).__init__(*args, **kwargs)
        self.limit = limit
        self.order_by = order_by
        self.url_detail = "https://www.planning.act.gov.au/applications-and-assessments/development-applications/browse-das/development-application-details"
        self.payloads_base = {
            "f": "json",
            "returnGeometry": "false",
            "outFields": "*",
            "resultRecordCount": self.limit,
            "where": "OBJECTID IS NOT NULL",
            "orderByFields": "DA_NUMBER DESC",
            "resultOffset": 0,
        }

    def start_requests(self):
        """
        添加请求负载
        """
        # 获取总数
        payloads_total = copy.copy(self.payloads_base)
        payloads_total["returnCountOnly"] = "true"
        for_count = requests.post(self.start_urls[0], data=payloads_total)
        total = json.loads(for_count.text)["count"]
        print("共有 {} 条结果".format(total))

        pages = int(total / self.limit) + 1

        for page in range(pages):
            # for page in range(1):
            print("get page{}".format(page + 1))
            # 为组装detail链接
            payloads_results = copy.copy(self.payloads_base)
            payloads_results["resultOffset"] = page * self.limit
            payloads_results["searchTerm"] = ""
            yield scrapy.Request(self.start_urls[0], method="POST", headers=self.headers, meta=payloads_results,
                                 dont_filter=True, body=urlencode(payloads_results))

    def parse(self, response: Response, **kwargs: any):
        self.logger.info(f'payload={response.meta}')
        useful_data = response.json()["features"]
        for info in useful_data:
            item = ACTItem()
            data = info["attributes"]

            if data["DA_NUMBER"]:
                da_number = data["DA_NUMBER"]
                # 用于在控制台输出，与用户交互
                print("\tget {}".format(da_number))
            else:
                continue
            item["da_number"] = "ACT-" + da_number
            item["address"] = data["STREET_ADDRESS"]
            item["description"] = data["PROPOSAL_TEXT"]
            item["district"] = data["DISTRICT"]
            item["suburb"] = data["SUBURB"]
            item["section"] = data["SECTION"]
            item["block"] = data["BLOCK"]
            item["organisation"] = data["COMPANYORG_NAME"]
            item["stage"] = data["DA_STAGE"]
            try:
                lodged_date = data["LODGEMENT_DATE"]
                item['lodgement_date'] = lodged_date if lodged_date else 0
            except:
                item['lodgement_date'] = 0
            try:
                lodged_date = data["DATE_END"]
                item['start_date'] = lodged_date if lodged_date else 0
            except:
                item['start_date'] = 0
            try:
                lodged_date = data["DATE_END"]
                item['end_date'] = lodged_date if lodged_date else 0
            except:
                item['end_date'] = 0
            #item["lodgement_date"] = self.ts_to_date(data["LODGEMENT_DATE"]) if data["LODGEMENT_DATE"] else ''
            #item["start_date"] = self.ts_to_date(data["DATE_START"]) if data["DATE_START"] else ''
            #item["end_date"] = self.ts_to_date(data["DATE_END"]) if data["DATE_END"] else ''
            item["application_amended"] = data["APPLICATION_AMENDED"]
            item["documents"] = self.get_docs(da_number)
            item['metadata']={}
            del item['metadata']
            yield item

    def get_docs(self, da_number):
        parmas = {
            "da-number": da_number,
            "amendment-version": ""
        }
        html = requests.get(url=self.url_detail, params=parmas)
        selector = Selector(text=html.text)
        docs_table = selector.css("div#main-content div.row table.da-table tr")[1:]
        docs = []
        for i in docs_table:
            doc_type = i.css('td:nth-child(1) strong::text').extract_first()
            doc_name = i.css('td:nth-child(2) a::text').extract_first().strip() if i.css(
                'td:nth-child(2) a::text').extract_first() is not None else "null"
            doc_url = i.css('td:nth-child(2) a::attr(href)').extract_first().strip()
            docs.append(doc_type + "@@" + doc_name + "@@" + doc_url)
        all_docs = ";".join(docs)
        return all_docs

    def ts_to_date(self, ts):
        # 将时间戳转换为秒数
        timestamp = int(ts) / 1000
        # 将秒数转换为日期时间对象
        date_time = datetime.datetime.fromtimestamp(timestamp)
        # 格式化日期时间对象为 "dd/MM/yyyy" 格式
        formatted_date = date_time.strftime('%d/%m/%Y')
        return formatted_date
