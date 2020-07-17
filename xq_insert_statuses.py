# coding=utf-8
"""使用时调用main(up,down) up为最大id down为最小id 程序补全其间的所有帖子
若不加参数 则默认为更新爬取
"""
import re
import time, json, requests
import pandas as pd
from sqlalchemy import create_engine
import os
import csv
import psycopg2

from copyheaders import headers_raw_to_dict
import logging
import random

# import StringIO python 2.X
import io

# read filename to record log
import sys
import os


def text_clean(text):
    # 去掉空行
    text0 = text.replace('</p><p>&nbsp;', '')
    text00 = text0.replace('</b></p><p><b>&nbsp;', '')
    # </p><p>代表要换行 插入换行转义符
    text1 = text00.replace('</p><p>', '</p> ' + chr(10) + '<p>')
    # 生成正则编译器，用于删除括号中的内容
    dr = re.compile(r'<[^>]+>', re.S)
    # 用正则编译器删去括号内容，删去空格符号，得到处理过后的文字
    processed_data = dr.sub('', text1).replace('&nbsp;', '')
    # 取前30个字符为标题
    title = processed_data[0:30]
    return title, processed_data


def get_max_id(table_name):
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    sql = """SELECT MAX(id) FROM {}""".format(table_name)
    cur.execute(sql)
    max_id = cur.fetchone()
    # commit
    conn.commit()
    return int(max_id[0])


def get_latest_id(headers_raw, cookies):
    url = """https://xueqiu.com/v4/statuses/home_timeline.json?source=user"""
    s = requests.session()
    headers = headers_raw_to_dict(headers_raw)
    r = s.get(url, headers=headers, cookies=cookies, allow_redirects=False, timeout=None)
    if r.cookies.get_dict():
        s.cookies.update(r.cookies)
    if r.status_code == 200:
        latest_id = r.json()['home_timeline'][0]['id']

        return int(latest_id)


def get_next_url(id):
    url = """https://xueqiu.com/v4/statuses/home_timeline.json?source=user&max_id={}""".format(str(id))
    return url


def catch_data(url, headers_raw, cookies):
    s = requests.session()
    headers = headers_raw_to_dict(headers_raw)
    r = s.get(url, headers=headers, cookies=cookies, allow_redirects=False, timeout=None)

    if r.cookies.get_dict():
        s.cookies.update(r.cookies)
    rows = r.json()['home_timeline']
    # time.sleep(random.randint(1, 2))
    if rows:
        return rows


def write_row(df, row):
    value_dict = {}

    for column in ["id", "user_id", "title", "text", 'source']:
        value_dict[column] = row[column]
    value_dict['created_timestamp'] = row['created_at']
    # add link
    for column in ["target"]:
        if row[column] == "":
            value_dict[column] = ""
        else:
            value_dict[column] = "https://xueqiu.com" + row[column]

    for b_level in ["screen_name", "verified_description", "city", "province"]:
        a_level = "user"
        ab = a_level + "_" + b_level
        value_dict[ab] = row[a_level][b_level]

    for b_level in ["id", "user_id", "title", "text", 'source']:
        a_level = "retweeted_status"
        ab = a_level + "_" + b_level
        try:
            value_dict[ab] = row[a_level][b_level]
        except:
            value_dict[ab] = ""

    for c_level in ["screen_name", "verified_description", "city", "province"]:
        a_level = "retweeted_status"
        b_level = "user"
        abc = a_level + "_" + b_level + "_" + c_level
        try:
            value_dict[abc] = row[a_level][b_level][c_level]
        except:
            value_dict[abc] = ""

    # stock correlation
    stocklist = ""
    if row["stockCorrelation"]:
        for sc in row["stockCorrelation"]:
            if stocklist == "":
                stocklist = sc
            else:
                stocklist = stocklist + "," + sc
    value_dict["stockCorrelation"] = stocklist

    stocklist_1 = ""
    if row["retweeted_status"]:

        for sc in row["retweeted_status"]["stockCorrelation"]:
            if stocklist_1 == "":
                stocklist_1 = sc
            else:
                stocklist_1 = stocklist_1 + "," + sc
    value_dict["retweeted_status_stockCorrelation"] = stocklist_1

    value_dict['processed_text'] = text_clean(value_dict['text'])[1]
    if not value_dict['title']:
        value_dict['title'] = text_clean(value_dict['text'])[0]

    df_line = pd.DataFrame(value_dict, index=[0])
    df = df.append(df_line, ignore_index=True)

    return df


def log():
    filename_log = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:] + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG, format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


def insert_value(up=0, down=0):
    # log file
    log()
    logging.info("running.")
    print("running.")

    # url variable
    url = "https://xueqiu.com/v4/statuses/home_timeline.json?source=user"

    headers_raw = b"""
        Accept: application/json, text/plain, */*
        Accept-Encoding: gzip, deflate, br
        Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
        Connection: keep-alive
        Host: xueqiu.com
        Referer: https://xueqiu.com/u/6344107619
        Sec-Fetch-Dest: empty
        Sec-Fetch-Mode: cors
        Sec-Fetch-Site: same-origin
        User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36
        """

    cookies = {
        'Cookie': 'aliyungf_tc=AQAAAJ4kVxrOCwgAxmMR2pr3vgx12DhM; snbim_minify=true; '
                  'device_id=24700f9f1986800ab4fcc880530dd0ed; Hm_lvt_1db88642e346389874251b5a1eded6e3=1592793404; '
                  's=cj137q59ip; __utmc=1; __utmz=1.1592793568.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); '
                  'remember=1; '
                  'xq_is_login=1; u=2764635162; bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; '
                  'captcha_id=QwFaqtJ13sksgsp9RatEnEOgh7qJXn; xq_a_token=84f14d8d58bc742ddbdfb3443523553d63a8025b; '
                  'xqat=84f14d8d58bc742ddbdfb3443523553d63a8025b; xq_r_token=9b4d7ef924beea602bac78e7eb97363f6f5edbeb; '
                  'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9'
                  '.eyJ1aWQiOjI3NjQ2MzUxNjIsImlzcyI6InVjIiwiZXhwIjoxNTk1MzkwNTM3LCJjdG0iOjE1OTI3OTg1MzcyNjQsImNpZCI6ImQ5ZDBuNEFadXAifQ.kVnwLdzuHrBpSuRQczcC13mNjqKGx6yUtnisXKaKcrrz_VNcFuZEAo7MlMBCDydipOJYmcVy2n45r3uoefagrOqPEAvHCBVjzjMNPZiALu42wbbfN9zrG7DsOAVnd8zlHidZLf0E7dNP1w_RtTKP7KY8nAcUFNWLYawTnGc3D75v7vW4nE2orUlFNPzm1DAaXeGG7e2LrNzgTWktIydCD_wxRvPuDd7ABF54xJKE-jljK0cAPriyvqztE09JvBW_Q7xvQm2sxnpr19xzWrU01tTPW4fEEKfw1xYSJnS3ePqF2X5GdJ1TzJHJXesUPoF6nYOdwLqDrDhSlJpyId_AKg; __utma=1.817981092.1592793568.1592798313.1592807818.3; acw_tc=2760825415928125915283497ecb0589c794e8b03b21c83f2bf712a19fe0a2; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1592812751 '

    }

    # database variable
    user = "dev1_db"
    password = "HdGY7MHZ6*v2"
    host = "pgm-8vb23fi81368zq0357540.pgsql.zhangbei.rds.aliyuncs.com:1433"
    db = "dev1"
    schema = 'public'
    engine = create_engine("postgresql+psycopg2://%s:%s@%s/%s" % (
        user, password, host, db))

    # table variable!!!
    table = "xq_values_demmmmo"
    df = pd.DataFrame()

    up1 = up
    down1 = down

    if up == 0:
        up1 = get_latest_id(headers_raw, cookies)
    if down == 0:
        down1 = get_max_id(table)

    if (not up1 == down1) and up == 0 and down == 0:
        url = """https://xueqiu.com/v4/statuses/home_timeline.json?source=user"""
        row = catch_data(url, headers_raw, cookies)
        df = write_row(df, row)

    try:
        while up1 > down1:
            url = get_next_url(up1)
            rows = catch_data(url, headers_raw, cookies)
            time.sleep(random.random()*3)
            for row in rows:
                if int(row['id']) > down1:
                    df = write_row(df, row)
                up1 = int(row['id'])

    except Exception:
        logging.error("ERROR!!!", exc_info=True)
    print df
    pd.io.sql.to_sql(df, table, engine, index=False, if_exists='append')
    print(' done')
    logging.info("finish.")
    print("finish.")


if __name__ == "__main__":
    insert_value()
