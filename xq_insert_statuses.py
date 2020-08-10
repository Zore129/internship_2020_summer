# coding=utf-8
"""只用做日常更新
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
    text1 = text.replace('</p><p>', '</p> ' + chr(10) + '<p>') \
        .replace('</p> <p>', '</p> ' + chr(10) + '<p>').replace('<br/>', chr(10))
    text2 = text1.replace('&nbsp;', ' ').replace('&quot;', '"').replace('&amp;', '&').replace('&#34;', '"')
    # 生成正则编译器，用于删除括号中的内容
    dr = re.compile(r'<[^>]+>', re.S)
    # 用正则编译器删去括号内容，删去空格符号，得到处理过后的文字
    processed_data = dr.sub('', text2)
    # 取前30个字符为标题
    title = processed_data[0:30]
    return title, processed_data


def get_max_id(table_name):
    conn = psycopg2.connect(database="demo", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    cur.execute("""set search_path to xueqiu""")
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
    value_dict['is_article'] = 1
    if not value_dict['title']:
        value_dict['title'] = text_clean(value_dict['text'])[0]
        value_dict['is_article'] = 0
    df_line = pd.DataFrame(value_dict, index=[0])
    df = df.append(df_line, ignore_index=True)

    return df


def log():
    filename_log = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:] + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG, format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


def insert_value():
    # log file
    log()
    logging.info("running.")
    print("running.")

    # url variable
    url = "https://xueqiu.com/v4/statuses/home_timeline.json?source=user"

    headers_raw = b"""
        Accept: */*
        Accept-Encoding: gzip, deflate, br
        Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
        Connection: keep-alive
        Host: xueqiu.com
        elastic-apm-traceparent: 00-74c09c1b6de4171e7789dc2a4fcd8024-8cac70c8730a5f01-00
        X-Requested-With: XMLHttpRequest
        Origin: https://xueqiu.com
        Referer: https://xueqiu.com/u/7295868403
        Sec-Fetch-Dest: empty
        Sec-Fetch-Mode: cors
        Sec-Fetch-Site: same-origin
        User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36
        """
    cookies = {
        'Cookie': 'device_id=24700f9f1986800ab4fcc880530dd0ed; s=cj137q59ip; __utmz=1.1592793568.1.1.utmcsr=('
                  'direct)|utmccn=(direct)|utmcmd=(none); bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; '
                  '__utma=1.817981092.1592793568.1594089109.1594608858.9; '
                  'aliyungf_tc=AQAAAMt/exjUOQwAxmMR2t/lmWrYwN42; Hm_lvt_1db88642e346389874251b5a1eded6e3=1594976484,'
                  '1595211780,1595325323,1595383383; remember=1; xq_a_token=915f95c7c9d7684c3e0b90134bcb3a682b5aa7d8; '
                  'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9'
                  '.eyJ1aWQiOjcyOTU4Njg0MDMsImlzcyI6InVjIiwiZXhwIjoxNTk3ODkzODQ0LCJjdG0iOjE1OTUzODM0MTYzOTYsImNpZCI6ImQ5ZDBuNEFadXAifQ.XFW5HfAuw-EoIv5234FgwXHVtkA4yRHR4xxZD1LQ-YyUs2dYpdWiGL_zsPQz1-a0v9jZ-bonP0RWBGPOvo2FlxbqTnzRKbOH_j1GppvHJlX81qbj0AEohtHE934W06bGOmOgSDE_oUD_4iJ7OL3wxvdcXYHwhKWKzkIkx4_rtNdqUAm9oD5JgBBRi_ihRvfBtqLBEILm9pwRcpn0QKK8L5pQ22Q2I3jgy7JjU81_zsDx4hVXhiOqRf5UE_3DH0C9HXRbNU1wbLiob_V30eWzphRB-gc6fTXh5xBhDzEhUk3F5Avg_L71df8TzsDyiHt1HIpCh7n-GMmrEnuEUPmm6g; xqat=915f95c7c9d7684c3e0b90134bcb3a682b5aa7d8; xq_r_token=8fbff5480d8a5e5068ad26f2cde2e66b33e1c661; xq_is_login=1; u=7295868403; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1595385044; acw_tc=2760820015953853892928326e46bd1043c8a4ae0fc4d130a08b4cbe3f0a6b'}

    # database variable
    user = "dev1_db"
    password = "HdGY7MHZ6*v2"
    host = "pgm-8vb23fi81368zq0357540.pgsql.zhangbei.rds.aliyuncs.com:1433"
    db = "demo"
    schema = 'xueqiu'
    engine = create_engine("postgresql+psycopg2://%s:%s@%s/%s" % (
        user, password, host, db))

    # table variable!!!
    table = "xq_bigv_statuses"
    df = pd.DataFrame()

    up1 = get_latest_id(headers_raw, cookies)
    down1 = get_max_id(table)

    # url = """https://xueqiu.com/v4/statuses/home_timeline.json?source=user"""
    # rows = catch_data(url, headers_raw, cookies)
    # df = write_row(df, rows[0])

    try:
        while up1 > down1:
            url = get_next_url(up1)
            rows = catch_data(url, headers_raw, cookies)
            time.sleep(1 + random.random() * 3)
            for row in rows:
                if int(row['id']) > down1:
                    df = write_row(df, row)
                up1 = int(row['id'])

    except Exception:
        logging.error("ERROR!!!", exc_info=True)
    print(df)
    pd.io.sql.to_sql(df, table, engine, schema=schema, index=False, if_exists='append')
    print(' done')
    logging.info("finish.")
    print("finish.")


if __name__ == "__main__":
    insert_value()
