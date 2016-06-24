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


# 获取当前的时间戳
def get_timstamp():
    timestamp = int(int(time.time()) / 30)
    return str(timestamp)


# 获取总页数
def get_pages_count():
    url = '''http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page=%d''' % 1
    url += "&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt="
    url += get_timstamp()
    wp = urllib.urlopen(url)
    data = wp.read().decode("gbk")
    start_pos = data.index('=')
    json_data = data[start_pos + 1:]
    result_dict = json.loads(json_data)
    pages = result_dict['pages']
    return pages


def convert_to_date(date_str):
    date_str = date_str.strip()
    if re.match("\d{4}-\d{2}-\d{2}", date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    return np.nan


def optimize_df(df):
    columns = [
        "SCode", "SName", "StartDate", "EndDate", "NoticeDate", "Description", "CompanyName", "OrgName",
        "CompanyCode", "Licostaff", "ChangePercent", "Place", "OrgCode", "OrgtypeName", "Orgtype", "Personnel",
    ]
    df = df.ix[:, columns]
    df['StartDate'] = df['StartDate'].map(lambda x: convert_to_date(x) if pd.notnull(x) else x)
    df['EndDate'] = df['EndDate'].map(lambda x: convert_to_date(x) if pd.notnull(x) else x)
    df['NoticeDate'] = df['NoticeDate'].map(lambda x: convert_to_date(x) if pd.notnull(x) else x)
    df['ChangePercent'] = pd.to_numeric(df['ChangePercent'], errors='coerce')
    return df


def build_table():
    page_count = get_pages_count()

    con = MySQLdb.connect(host="192.168.0.114", port=3306, user="root", passwd="fit123456", charset="utf8",
                          db="pachong")
    browser = webdriver.Chrome()
    for page in xrange(1, page_count + 1):
        start = time.clock()
        url = '''http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page=%d''' % page
        url += "&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt="
        url += get_timstamp()
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
        print page ," of ",page_count," complete!"
        end = time.clock()
        print("The function run time is : %.03f seconds" % (end - start))


if __name__ == "__main__":
    build_table()
