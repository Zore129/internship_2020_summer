# coding=utf-8
import json
import logging
import os
import random
import re
import sys
import time

import pandas as pd
import psycopg2
import requests
from copyheaders import headers_raw_to_dict
from sqlalchemy import create_engine


def log():
    filename_log = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:] + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG, format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


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
    value_dict['is_article'] = 0
    if not value_dict['title']:
        value_dict['title'] = text_clean(value_dict['text'])[0]
        value_dict['is_article'] = 1
    df_line = pd.DataFrame(value_dict, index=[0])
    print(df_line)
    df = df.append(df_line, ignore_index=True)

    return df


def get_bigv_statuses_data(uid, df, timestamp):
    # store userinfo to update
    url = "https://xueqiu.com/v4/statuses/user_timeline.json?page=1&user_id={}".format(uid)

    headers_raw = b"""
        Accept: application/json, text/plain, */*
        Accept-Encoding: gzip, deflate, br
        Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
        Connection: keep-alive
        Host: xueqiu.com
        elastic-apm-traceparent: 00-74c09c1b6de4171e7789dc2a4fcd8024-8cac70c8730a5f01-00
        X-Requested-With: XMLHttpRequest
        Origin: https://xueqiu.com
        Sec-Fetch-Dest: empty
        Sec-Fetch-Mode: cors
        Sec-Fetch-Site: same-origin
        User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36
        """
    headers = headers_raw_to_dict(headers_raw)
    cookies = {
        'Cookie': 'device_id=24700f9f1986800ab4fcc880530dd0ed; s=cj137q59ip; __utmz=1.1592793568.1.1.utmcsr=('
                  'direct)|utmccn=(direct)|utmcmd=(none); bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; '
                  '__utma=1.817981092.1592793568.1594089109.1594608858.9; '
                  'aliyungf_tc=AQAAAK+wG3yPCQwAxmMR2nQtRXxhCPrt; Hm_lvt_1db88642e346389874251b5a1eded6e3=1594972560,'
                  '1594976484,1595211780,1595325323; snbim_minify=true; '
                  'xq_a_token=915f95c7c9d7684c3e0b90134bcb3a682b5aa7d8; '
                  'xqat=915f95c7c9d7684c3e0b90134bcb3a682b5aa7d8; '
                  'xq_r_token=8fbff5480d8a5e5068ad26f2cde2e66b33e1c661; '
                  'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9'
                  '.eyJ1aWQiOjcyOTU4Njg0MDMsImlzcyI6InVjIiwiZXhwIjoxNTk3ODkzODQ0LCJjdG0iOjE1OTUzMjYxMzM5ODAsImNpZCI6ImQ5ZDBuNEFadXAifQ.Bw7frr3jRZR3DGBk7jvhMeMXPCnCgUyhFcn8va4hVpeC6Thc0KSu8QSk1SATU4ZnMc14ailVgB2-jZ7FJ7EBZ0GCasEAq6R6OfFpSzeP4UspXhmncAGt2Zl9dtiCi5nZz5qXalkzfU3zmEwWy-6Agsy8diMooMNPgU5FYUYlN_b2T5ARZSnQTjldVOrfRZFkp9GcRyWY6cCjfnpJjo1hu7G471uZ8J08tBTLwogHSX4eYmIEXQunfCb7H7J4D2A4YqDljv_DGY3IyaTdmteERoM8tmgxgeEnPKRWNzn7lehlE0_bkI_GqReHExMCGHbnrTcnmDGPmg3Dr7o3ZHV0Zw; xq_is_login=1; u=7295868403; acw_tc=2760821815953271886532536e9869980c640fdf941b98fd28e81841c77a4b; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1595328040 '
    }
    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    # print(r.status_code)
    # print(r.headers)
    dictstr = json.loads(r.text)
    maxPage = dictstr['maxPage']
    if maxPage != 0:
        for pagenum in range(maxPage):
            url = "https://xueqiu.com/v4/statuses/user_timeline.json?user_id={}&page={}".format(uid, pagenum + 1)
            print(url)
            r = requests.get(url, headers=headers, cookies=cookies,
                             allow_redirects=False, timeout=None)
            dic = json.loads(r.text)
            time.sleep(2 + random.random() * 3)
            # print(dic)
            for k in dic['statuses']:
                if k['mark'] == 1:
                    if int(k['created_at']) > timestamp:
                        df = write_row(df, k)
                        print('write')
                else:
                    if int(k['created_at']) < timestamp:
                        return df
                    df = write_row(df, k)
                    print('write')

            print(str(pagenum) + "____done")
        return df


def get_uid_from_web():
    bigv_id_list = []
    url = "https://xueqiu.com/friendships/groups/members.json?uid=7295868403&page=1&gid=0"

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
    headers = headers_raw_to_dict(headers_raw)
    cookies = {
        'Cookie': 'device_id=24700f9f1986800ab4fcc880530dd0ed; s=cj137q59ip; __utmz=1.1592793568.1.1.utmcsr=('
                  'direct)|utmccn=(direct)|utmcmd=(none); bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; '
                  '__utma=1.817981092.1592793568.1594089109.1594608858.9; '
                  'aliyungf_tc=AQAAAMt/exjUOQwAxmMR2t/lmWrYwN42; Hm_lvt_1db88642e346389874251b5a1eded6e3=1594976484,'
                  '1595211780,1595325323,1595383383; remember=1; xq_a_token=915f95c7c9d7684c3e0b90134bcb3a682b5aa7d8; '
                  'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9'
                  '.eyJ1aWQiOjcyOTU4Njg0MDMsImlzcyI6InVjIiwiZXhwIjoxNTk3ODkzODQ0LCJjdG0iOjE1OTUzODM0MTYzOTYsImNpZCI6ImQ5ZDBuNEFadXAifQ.XFW5HfAuw-EoIv5234FgwXHVtkA4yRHR4xxZD1LQ-YyUs2dYpdWiGL_zsPQz1-a0v9jZ-bonP0RWBGPOvo2FlxbqTnzRKbOH_j1GppvHJlX81qbj0AEohtHE934W06bGOmOgSDE_oUD_4iJ7OL3wxvdcXYHwhKWKzkIkx4_rtNdqUAm9oD5JgBBRi_ihRvfBtqLBEILm9pwRcpn0QKK8L5pQ22Q2I3jgy7JjU81_zsDx4hVXhiOqRf5UE_3DH0C9HXRbNU1wbLiob_V30eWzphRB-gc6fTXh5xBhDzEhUk3F5Avg_L71df8TzsDyiHt1HIpCh7n-GMmrEnuEUPmm6g; xqat=915f95c7c9d7684c3e0b90134bcb3a682b5aa7d8; xq_r_token=8fbff5480d8a5e5068ad26f2cde2e66b33e1c661; xq_is_login=1; u=7295868403; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1595385044; acw_tc=2760820015953853892928326e46bd1043c8a4ae0fc4d130a08b4cbe3f0a6b'}
    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    dictstr = json.loads(r.text)
    maxPage = dictstr['maxPage']
    print(maxPage)
    for pagenum in range(maxPage):
        url = "https://xueqiu.com/friendships/groups/members.json?uid=7295868403&page={}&gid=0".format(pagenum + 1)
        print(url)
        r = requests.get(url, headers=headers, cookies=cookies,
                         allow_redirects=False, timeout=None)
        dic = json.loads(r.text)
        time.sleep(1 + random.random() * 3)
        # print(dic)
        for k in dic['users']:
            bigv_id_list.append(k['id'])
        print(str(pagenum) + "____done")
    return bigv_id_list


if __name__ == "__main__":
    log()
    logging.info("running.")
    print("running.")
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /demo""")
    for row in get_uid_from_web():
        df = pd.DataFrame()
        print(row)
        df = get_bigv_statuses_data(row, df, timestamp=1577808000000)
        pd.io.sql.to_sql(df, 'xq_bigv_statuses_test', engine, schema='xueqiu', index=False, if_exists='append')
    # pd.io.sql.to_sql(df, 'xq_bigv_statuses_test', engine, schema='xueqiu',index=False, if_exists='append')
    logging.info("finish.")
    print("finish.")
