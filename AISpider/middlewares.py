# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import time
from scrapy import signals
from scrapy.http import HtmlResponse
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class AispiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class AispiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class RandomUserAgentMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def __init__(self, crawler):
        super(RandomUserAgentMiddleware, self).__init__()
        self.ua = UserAgent()
        self.ua_type = crawler.settings.get('random_ua_type', 'random')

    def process_request(self, request, spider):
        def get_ua():
            try:
                return getattr(self.ua, self.ua_type)
            except:
                return getattr(self.ua, 'random')

        request.headers.setdefault('User-Agent', get_ua())
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class SeleniumMiddleware:

    """
    通过中间件使用 Selenium 的示例
    """

    def process_request(self, request, spider):
        fist_page = None
        count_list = []
        selenium_flag = request.meta.get('selenium')
        date_range = request.meta.get('date_range')
        page_size = 25 #每页最多显示25条
        total = request.meta.get('total')
        current_page = 1 or request.meta.get('current_page')
        if total:
            fist_page, count_list = self.cal_page(total, current_page)
        if selenium_flag:
            if total is None:
                #todo:分页的逻辑暂时没有先用晚点添加
                pass
            browser = spider.browser
            browser.get(request.url)
            wait = WebDriverWait(browser, 10)

            # 点击“Advanced”以展开选项
            advanced_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//h2[text()='Advanced']")))
            advanced_button.click()

            last_month_option = wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//input[@name='dateRange'][@value='lastMonth']")))
            last_month_option.click()

            # 点击“Search”按钮
            search_button = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "div.accordion__item.accordion-opened input[type='submit'][value='Search']")))
            search_button.click()
            time.sleep(5)

            return HtmlResponse(request.url, body=browser.page_source, encoding='utf-8', request=request)

    def cal_page(self, total, target,):
        count_list = []
        page_list = [i+1 for i in range(total)]
        if total/2>target:
            #从后往前点
            fist_page = 'Last'
            if target in page_list[-5:]:
                count_list.append(target)
            else:
                count_list.append(page_list[-5])
                for i in list(reversed(page_list[:-5]))[1::2]:
                    if target >= i:
                        count_list.append(target)
                        break
                    else:
                        count_list.append(i)
        else:
            #从前往后
            fist_page = 'First'
            if target in page_list[:5]:
                count_list.append(target)
            else:
                count_list.append(page_list[4])
                for i in page_list[5:][1::2]:
                    if target <= i:
                        count_list.append(target)
                        break
                    else:
                        count_list.append(i)
        return fist_page, count_list



