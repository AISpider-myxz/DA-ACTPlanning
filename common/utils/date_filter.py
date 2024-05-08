import datetime
import time


class DateFilter:
    """
    时间过滤器
    以下6个方法用于获取时间戳，均是在处理developmenti网站时，为处理时间范围而设计的
        get_today: 获取今天的 23:59:59 的时间戳
        get_yesterday: 获取昨天的 23:59:59 的时间戳
        get_lastmonth_final: 获取上个月最后一天的 23:59:59 的时间戳
        get_thismonth_start: 获取本月的第一天的 00:00:00 的时间戳
        get_startdate: 获取开始日期
        get_date: 获取指定日期的 23:59:59 的时间戳

    以下1个方法用于将时间戳转换为sql日期，可为其他爬虫在进行时间范围查询时使用，格式为：YYYY-MM-DD
        get_sqldate: 将时间戳转换为sql日期

    以下1个方法用于获取指定日期的几个月前的日期，用于处理Moreton Bay网站时，为处理时间范围而设计的
        get_month_ago_date: 获取指定日期的几个月前的日期


    """

    def __init__(self):
        pass

    def get_today(self) -> int:
        """
        获取今天的 23:59:59 的时间戳
        """
        # 获取当前日期
        today = datetime.date.today()
        # 构建今天的23:59:59时间
        end_of_day = datetime.datetime.combine(today, datetime.time(23, 59, 59))
        # 将datetime对象转换为时间戳
        return int(time.mktime(end_of_day.timetuple()) * 1000 + 999)

    def get_yesterday(self) -> int:
        """
        获取昨天的 23:59:59 的时间戳
        """
        # 获取当前日期
        today = datetime.date.today()
        # 获取昨天的日期
        yesterday = today - datetime.timedelta(days=1)
        # 构建昨天的23:59:59时间
        end_of_day = datetime.datetime.combine(yesterday, datetime.time(23, 59, 59))
        # 将datetime对象转换为时间戳
        return int(time.mktime(end_of_day.timetuple()) * 1000 + 999)

    def get_lastmonth_final(self, date=int(time.time() * 1000)) -> int:
        """
        获取上个月最后一天的 23:59:59 的时间戳
        """
        # 获取当前日期
        # today = datetime.date.today()
        today = datetime.datetime.fromtimestamp(float(date / 1000))
        # 获取上个月的最后一天
        last_month = today.replace(day=1) - datetime.timedelta(days=1)
        # 构建上个月的23:59:59时间
        end_of_day = datetime.datetime.combine(last_month, datetime.time(23, 59, 59))
        # 将datetime对象转换为时间戳
        return int(time.mktime(end_of_day.timetuple()) * 1000 + 999)

    def get_thismonth_start(self, timestamp: int) -> int:
        """
        获取本月的第一天的 00:00:00 的时间戳
        :param timestamp: 时间戳
        """
        # 获取当前日期
        today = datetime.datetime.fromtimestamp(float(timestamp / 1000))
        # 获取本月的第一天
        first_day = today.replace(day=1)
        # 构建本月的00:00:00时间
        start_of_day = datetime.datetime.combine(first_day, datetime.time(0, 0, 0))
        # 将datetime对象转换为时间戳
        return int(time.mktime(start_of_day.timetuple()) * 1000)

    def get_startdate(self, date: int, rangedate) -> int:
        """
        获取开始日期
        :param date: 日期
        :param rangedate: 范围
        :return: 开始日期
        """
        today = datetime.datetime.fromtimestamp(date / 1000)
        # 指定天数以前的日期
        days_ago = today - datetime.timedelta(days=rangedate)
        # 构建指定日期的00:00:00时间
        start_of_day = datetime.datetime.combine(days_ago, datetime.time(0, 0, 0))
        # 将datetime对象转换为时间戳
        return int(time.mktime(start_of_day.timetuple()) * 1000)

    def get_date(self, date) -> int:
        """
        获取指定日期的 23:59:59 的时间戳
        :param date: 日期
        :return: 23:59:59 的时间戳
        """
        # 构建指定日期的23:59:59时间
        end_of_day = datetime.datetime.combine(date, datetime.time(23, 59, 59))
        # 将datetime对象转换为时间戳
        return int(time.mktime(end_of_day.timetuple()) * 1000 + 999)

    def get_sqldate(self, date: int) -> str:
        """
        将时间戳转换为sql日期
        :param date: 时间戳
        :return: sql日期，格式为"2024-01-21"
        """
        timeArray = time.localtime(date / 1000)
        return time.strftime("%Y-%m-%d", timeArray)


    def get_month_ago_date(slef, date: str, ago: int) -> str:
        """
        获取指定日期的几个月前的日期
        :param date: 日期
        :param ago: 月份
        :return: 几个月前的日期
        """
        big_month = ["01", "03", "05", "07", "08", "10", "12"]
        small_month = ["04", "06", "09", "11"]
        year = int(date[:4])
        month = int(date[5:7])
        day = int(date[8:10])
        # ago可能大于12
        year -= ago // 12
        ago = ago % 12
        if month - ago <= 0:
            year -= 1
            month = 12 + month - ago
        else:
            month -= ago
        if str(month) in big_month:
            if day > 31:
                day = 31
        elif str(month) in small_month:
            if day > 30:
                day = 30
        else:
            if year % 4 == 0 and year % 100 != 0 or year % 400 == 0:
                if day > 29:
                    day = 29
            else:
                if day > 28:
                    day = 28
        return f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}"