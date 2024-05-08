import typing
from datetime import date, datetime, timedelta

DATE_FORMATE = '%Y-%m-%d'  # "%d/%m/%Y"


def get_all_month(dt: typing.Union[date, str], date_format='%Y-%m-%d'):
    """
    获取从指定时间开始到现在的所有月份
    日期从每个月1号开始计算
    """
    monthes = []
    if isinstance(dt, str):
        try:
            dt = datetime.strptime(dt, date_format)
        except:
            return
    now = datetime.now()
    for year in range(dt.year, now.year + 1):
        if year == now.year:
            monthes.extend([date(year, i, 1).strftime(date_format) for i in range(1, now.month + 2)])
        elif year == dt.year:
            monthes.extend([date(year, i, 1).strftime(date_format) for i in range(dt.month, 13)])
        else:
            monthes.extend([date(year, i, 1).strftime(date_format) for i in range(1, 13)])
    return monthes

def get_last_days(start_date_str, date_format='%Y-%m-%d'):
    # 将字符串形式的日期转换为 datetime 对象
    start_date = datetime.strptime(start_date_str, date_format).date()

    end_date = datetime.now().date()
    last_days = []

    while start_date <= end_date:
        # Find the last day of the current month
        next_month = start_date.replace(day=28) + timedelta(days=4)
        last_day_of_month = next_month - timedelta(days=next_month.day)

        # Append the last day of the current month to the list
        last_days.append(last_day_of_month.strftime(date_format))

        # Move to the next month
        start_date = last_day_of_month + timedelta(days=1)

    # 如果最后一个日期大于当前日期，则用当前日期替换
    if last_days[-1] > end_date.strftime(date_format):
        last_days[-1] = end_date.strftime(date_format)

    return last_days


if __name__ == '__main__':
    all_month_start = get_all_month('01/01/1991', "%d/%m/%Y")
    # all_month_end = get_last_days('2016-06-01')
    print(all_month_start)
    # print(all_month_end)
    print(len(all_month_start))
    # print(len(all_month_end))
