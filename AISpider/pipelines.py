# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import json
import os
import time

import pymongo
import logging
import copy
from pathlib import Path
from uuid import uuid4
from urllib.parse import quote_plus
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymysql.cursors import DictCursor
from twisted.enterprise import adbapi
from scrapy.exceptions import DropItem

import pymysql
from crawlab import save_item
from crawlab.config import get_task_id
from crawlab.entity.result import Result

def get_shot_uuid(n=63):
    def numberToBase(n, b):
        if n == 0:
            return [0]
        digits = []
        while n:
            digits.append(int(n % b))
            n //= b
        return digits[::-1]

    # urlsafe_66_alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-.~'
    file_name_alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_'
    if n > len(file_name_alphabet):
        n = len(file_name_alphabet)
    return ''.join(file_name_alphabet[x] for x in numberToBase(uuid4().int, n))

class MysqlScrapyPipeline(object):

        def __init__(self,):  
            self.db = pymysql.connect(host='192.168.0.110',
                        user='root',
                        password='123456',
                        database='spider',
                        port=13306)
        
            self.cursor = self.db.cursor()

        def process_item(self,item,spider):
            try: 
                self.save_data_2_db(cursor=self.cursor,item=item)
                self.db.commit()
                print('commit')

                result = Result(item)
                result.set_task_id(get_task_id())
                save_item(result)

                return item
            except:
                self.db.rollback()
                return 'Fail'

        def handle_error(self, failure, item, spider):
            print(failure)

        def save_data_2_db(self,cursor,item):
            adapter = ItemAdapter(item)
            adapter = adapter.asdict()
            metadata = adapter.pop('metadata', {})
            table_name = item.get_table_name()
            unique = item.get_unique_fields()
            dbcall_func = getattr(item, 'get_express_sql', None)
            if item.get('operator_name') == 'Uniting Account':
                a = 1
            if dbcall_func:
                sql_str, params = dbcall_func()
                self.cursor.execute(sql_str, params)
            else:
                unique_list = []
                for field in unique:
                    val = item.get(field)
                    if val is None:
                        condition = f'%s = NULL' % field
                    else:
                        condition = f'%s = "%s"' % (field, item.get(field))
                    unique_list.append(condition)
                where_cluse = 'WHERE ' + ' AND '.join(unique_list)
                select_cluse = f'SELECT * FROM {table_name}'
                sql_str = f'{select_cluse} {where_cluse};'
                records = self.do_get(self.cursor, sql_str)
                if records:
                    if metadata.get('update', True):
                        update_list = []
                        for key, val in adapter.items():
                            if key in unique:
                                continue
                            else:
                                if val is None:
                                    update_list.append(f'%s = NULL' % key)
                                else:
                                    if isinstance(val, str) and '"' in str(val):
                                        val = val.replace('\"', '""')
                                    update_list.append(f'%s = "%s"' % (key, val))
                        drop_cluse = f'DELETE FROM {table_name}'
                        
                        sql_str = f'{drop_cluse} {where_cluse}'
                        print(sql_str)
                        self.cursor.execute(sql_str)
                    else:
                        duplicates = []
                        for field in unique:
                            duplicates.append(f'{field}<{item.get(field)}>')
                        raise DropItem("Duplicate item found: %s" % ','.join(duplicates))
                else:
                    pass
        def do_get(self, cursor, sql_string):
            cursor.execute(sql_string)
            return cursor.fetchall()

class FieldsPipline(object):

    def process_item(self, item, spider):
        fields_saved = item.get_save_fields()
        path_dir = f'media/{spider.name}/fields'
        file_name = f"{get_shot_uuid()}_{str(time.time()).replace('.', '')}.json"
        if not fields_saved:
            return item
        path = Path(__file__).parent / path_dir
        if not path.exists():
            os.makedirs(path, exist_ok=True)
        item_dict = {}
        for field in fields_saved:
            val = item.get(field)
            if val:
                item_dict[field] = val
                item[field] = f'{path_dir}/{file_name}'
        if not item_dict:
            return item
        with open(path / file_name, 'w') as f:
            json.dump(item_dict, f)
        return item
