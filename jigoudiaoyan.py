# __author__ = 'fit'
# -*- coding: utf-8 -*-
from selenium import webdriver
import time
import urllib
import json
import MySQLdb
import pandas as pd
import re
import numpy as np
import datetime


class get_jigoudiaoyan:
    # 获取当前的时间戳
    def get_timstamp(self):
        timestamp = int(int(time.time()) / 30)
        return str(timestamp)

    # 获取总页数
    def get_pages_count(self):
        url = '''http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page=%d''' % 1
        url += "&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt="
        url += self.get_timstamp()
        wp = urllib.urlopen(url)
        data = wp.read().decode("gbk")
        start_pos = data.index('=')
        json_data = data[start_pos + 1:]
        result_dict = json.loads(json_data)
        pages = result_dict['pages']
        return pages

    def convert_to_date(self, date_str):
        date_str = date_str.strip()
        if re.match("\d{4}-\d{2}-\d{2}", date_str):
            return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        return np.nan

    def optimize_df(self, df):
        columns = [
            "SCode", "SName", "StartDate", "EndDate", "NoticeDate", "Description", "CompanyName", "OrgName",
            "CompanyCode", "Licostaff", "ChangePercent", "Place", "OrgCode", "OrgtypeName", "Orgtype", "Personnel",
        ]
        df = df.ix[:, columns]
        df['StartDate'] = df['StartDate'].map(lambda x: self.convert_to_date(x) if pd.notnull(x) else x)
        df['EndDate'] = df['EndDate'].map(lambda x: self.convert_to_date(x) if pd.notnull(x) else x)
        df['NoticeDate'] = df['NoticeDate'].map(lambda x: self.convert_to_date(x) if pd.notnull(x) else x)
        df['ChangePercent'] = pd.to_numeric(df['ChangePercent'], errors='coerce')
        return df

    # 获取当前数据表中最新数据的日期
    def get_lastest_date(self):
        con = MySQLdb.connect(host="192.168.0.114", user="root", passwd="fit123456", port=3306, charset="utf8",
                              db="pachong")
        sql = "select NoticeDate from jigoudiaoyan_new order by jigoudiaoyan_new desc limit 0,1"
        cur = con.cursor()
        cur.execute(sql)
        result = cur.fetchone()
        return result[0]

    def build_table(self):
        page_count = self.get_pages_count()

        con = MySQLdb.connect(host="192.168.0.114", port=3306, user="root", passwd="fit123456", charset="utf8",
                              db="pachong")
        browser = webdriver.PhantomJS('/home/fit/.linuxbrew/lib/node_modules/phantomjs/lib/phantom/bin/phantomjs')
        for page in xrange(1, page_count + 1):
            start = time.clock()
            url = '''http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page=%d''' % page
            url += "&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt="
            url += self.get_timstamp()
            browser.get(url)
            # 利用xpath表达式定位到标签
            pre = browser.find_element_by_xpath("//pre")
            data = pre.text
            start_pos = data.index("=")
            json_data = data[start_pos + 1:]
            result_dict = json.loads(json_data)
            result_list = result_dict['data']
            df = pd.DataFrame(result_list)
            pd.io.sql.to_sql(df, 'jigoudiaoyan_new', con, flavor='mysql', if_exists='append', index=False)
            print page, " of ", page_count, " complete!"
            end = time.clock()
            print("The function run time is : %.03f seconds" % (end - start))

    # upsert数据,表中没有的数据才插入
    def upsert_data(self, df, lastest_date):
        con = MySQLdb.connect(host="192.168.0.114", port=3306, user="root", passwd="fit123456", charset="utf8",
                              db="pachong")
        sql = "select * from jigoudiaoyan_new where NoticeDate='%s' " % lastest_date
        df_remain = pd.read_sql(sql, con=con)
        remain_set = set(map(tuple, df_remain.values))
        all_set = set(map(tuple, df.values))
        divide_set = all_set - remain_set
        if len(divide_set) == 0:
            return
        update_list = map(list, divide_set)
        columns = [
            "SCode", "SName", "StartDate", "EndDate", "NoticeDate", "Description", "CompanyName", "OrgName",
            "CompanyCode", "Licostaff", "ChangePercent", "Place", "OrgCode", "OrgtypeName", "Orgtype", "Personnel",
        ]
        update_df = pd.DataFrame(update_list, columns=columns)
        pd.io.sql.to_sql(update_df, 'jigoudiaoyan_new', con, flavor='mysql', if_exists='append', index=False)

    def refresh_table(self):
        page_count = self.get_pages_count()
        con = MySQLdb.connect(host="192.168.0.114", port=3306, user="root", passwd="fit123456", charset="utf8",
                              db="pachong")
        browser = webdriver.PhantomJS('/home/fit/.linuxbrew/lib/node_modules/phantomjs/lib/phantom/bin/phantomjs')
        lastest_date = self.get_lastest_date()
        for page in xrange(1, page_count + 1):
            start = time.clock()
            url = '''http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page=%d''' % page
            url += "&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt="
            url += self.get_timstamp()
            browser.get(url)
            # 利用xpath表达式定位到标签
            pre = browser.find_element_by_xpath("//pre")
            data = pre.text
            start_pos = data.index("=")
            json_data = data[start_pos + 1:]
            result_dict = json.loads(json_data)
            result_list = result_dict['data']
            df = pd.DataFrame(result_list)
            df_update = df[df['NoticeDate'] >= lastest_date]
            df_new = df[df['NoticeDate'] > lastest_date]
            df_that_day = df[df['NoticeDate'] == lastest_date]
            if len(df_that_day) != 0:
                pass
            if len(df_new) != 0:
                pd.io.sql.to_sql(df_new, 'jigoudiaoyan_new', con, flavor='mysql', if_exists='append', index=False)
            if len(df_update) != len(df):
                break


if __name__ == "__main__":
    crawler = get_jigoudiaoyan()
    crawler.refresh_table()
