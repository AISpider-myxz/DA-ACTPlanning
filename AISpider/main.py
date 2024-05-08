import sys
import os
from scrapy.cmdline import execute


# 获取当前文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))  # 当前文件的绝对路径，然后再找他的父级目录
sys.path.append(current_dir)  # 将当前路径加入到path中
os.chdir(current_dir)# 跳转到当前文件所在目录

# execute(['celery','-A','celery_app','worker','-l','info','-n','celery@get_detial'])
# execute(['celery -A celery_app worker -l info -n celery@get_detial'])
execute(['scrapy', 'crawl', 'act'])
