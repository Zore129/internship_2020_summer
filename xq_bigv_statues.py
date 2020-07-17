# coding=utf-8
import json
import random
import re
import time

import pandas as pd
import psycopg2
import requests
from copyheaders import headers_raw_to_dict
from sqlalchemy import create_engine


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


def get_bigv_statuses_data(uid, df, timestamp):
    # store userinfo to update
    url = "https://xueqiu.com/v4/statuses/user_timeline.json?page=1&user_id={}".format(uid)

    headers_raw = b"""
                Accept: */*
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
    headers = headers_raw_to_dict(headers_raw)
    cookies = {
        'Cookie': 'device_id=24700f9f1986800ab4fcc880530dd0ed;'
                  ' s=cj137q59ip;'
                  ' __utmz=1.1592793568.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); '
                  'bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; '
                  '__utma=1.817981092.1592793568.1594089109.1594608858.9;'
                  'aliyungf_tc=AQAAANG9cWmA3gIAxmMR2oZTY60bn1q/; '
                  'snbim_minify=true; remember=1; xq_a_token=84f14d8d58bc742ddbdfb3443523553d63a8025b; '
                  'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9'
                  '.eyJ1aWQiOjI3NjQ2MzUxNjIsImlzcyI6InVjIiwiZXhwIjoxNTk1MzkwNTM3LCJjdG0iOjE1OTQ5NTg2NDA2NjMsImNpZCI6ImQ5ZDBuNEFadXAifQ.Xy2Aie_k5diROfUqbs1CVmmXWG-U6HjiYvwX_edd5Ygensa5yPYcvp3WVgS8hqQ3jDcwwtVU6UTCwHGU0mIbNPfPEFzRk6SabKRhtfEblM2j0ldjlK54N5etVZ3MyDCaFphd_ylMnyVjrJJDEvN0PxPQ8UD4AJUuTu1J9ND6E4Y6fHkOt0c0JzQkCo8i7bcFU3ys6zRMM6tQNnpLuLGliqSDoA6fdHUj1lcBMr_VfhOnvemmhPM3v-hdT1rpY-sccTIT41IeBHhoDkpvLOOAwaxHOkldciAGdrnp7xta8OlwNZzbbZZaQma-4Hgbp2ZaPzp9G9dR6hIMTKqXnmczQQ; xqat=84f14d8d58bc742ddbdfb3443523553d63a8025b; xq_r_token=9b4d7ef924beea602bac78e7eb97363f6f5edbeb; xq_is_login=1; u=2764635162; Hm_lvt_1db88642e346389874251b5a1eded6e3=1594869365,1594881198,1594951757,1594968432; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1594968635; acw_tc=2760820e15949698335623055ecebfbe4c1c0be667f35eddd126a85454f9d2'}

    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    print r.status_code
    print (r.headers)
    dictstr = json.loads(r.text)
    maxPage = dictstr['maxPage']
    if maxPage != 0:
        for pagenum in range(maxPage):
            url = "https://xueqiu.com/v4/statuses/user_timeline.json?user_id={}&page={}".format(uid, pagenum + 1)
            print url
            r = requests.get(url, headers=headers, cookies=cookies,
                             allow_redirects=False, timeout=None)
            dic = json.loads(r.text)
            time.sleep(random.random()*3)
            # print(dic)
            for k in dic['statuses']:
                if int(k['created_at']) < timestamp:
                    print int(k['created_at'])
                    return df
                df = write_row(df, k)

            print(str(pagenum) + "____done")
        return df


def get_uid():
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    print("Opened database successfully")
    cur = conn.cursor()
    sql = """SELECT uid FROM xq_bigv"""
    cur.execute(sql)
    rows = cur.fetchall()
    # print rows
    # notice the structure of list as below
    # print(rows[0][0])
    return rows


if __name__ == "__main__":
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /dev1""")
    df = pd.DataFrame()
    for row in get_uid():
        get_bigv_statuses_data(row[0], df, timestamp=1594742400000)
    pd.io.sql.to_sql(df, 'xq_bigv_statuses', engine, index=False, if_exists='append')
