import datetime

def get_today():
    # 获取今日 格式：23/04/2024
    today = datetime.datetime.today().strftime("%d/%m/%Y")
    return today

def get_this_month():
    # 获取本月 格式01/04/2024
    today = datetime.datetime.today().strftime("%d/%m/%Y")
    today_list = today.split('/')
    today_list[0] = '01'
    today = '/'.join(today_list)
    return today

def get_next_month():
    # 获取下月 格式01/05/2024
    today = datetime.datetime.today().strftime("%d/%m/%Y")
    today_list = today.split('/')
    today_list[0] = '01'
    if str(int(today_list[1]) + 1) == '13' :
        today_list[1] = '01'
        today_list[2] = str(int(today_list[2])+1)
        today = '/'.join(today_list)
        return today
    else:
        today_list[1] = str(int(today_list[1]) + 1).zfill(2)
        today = '/'.join(today_list)
        return today
# print(get_this_month())
# print(get_next_month())
