# coding=utf-8
import csv
import json

import psycopg2
import requests
from copyheaders import headers_raw_to_dict
import pandas as pd
import time


def insert_bigv(uid):
    # 生成对应用户的url
    url = "https://xueqiu.com/v4/statuses/user_timeline.json?page=1&user_id={}".format(uid)

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
    # s = requests.session()

    headers = headers_raw_to_dict(headers_raw)

    # dictstr = json.loads(r.text)
    # maxPage = dictstr['maxPage']
    # print(maxPage)

    # 查询有哪些字段
    # for i in dictstr['statuses'][0]:
    #     print(i)
    #     print(dictstr['statuses'][0][i])
    #     print()

    # 写入数据

    # print(url)
    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    dic = json.loads(r.text)
    # print(dic)
    k = dic['statuses'][0]['user']
    print('____done')
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    sql = """insert into xq_bigv_copy1(uid, province, city, gender, friends_count, stocks_count, description, status_count,
           verified_description, verified_type, screen_name, verified, followers_count, donate_count,
           "maxPage") values
           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"""
    params = (
        uid, k['province'], k['city'], k['gender'], k['friends_count'], k['stocks_count'], k['description'],
        k['status_count'],
        k['verified_description'], k['verified_type'], k['screen_name'], k['verified'], k['followers_count'],
        k['donate_count'], dic['maxPage'],)
    # print(params)
    cur.execute(sql, params, )
    # commit
    conn.commit()
    pass


if __name__ == "__main__":
    f = pd.read_csv('user_id.csv')
    for z in f['id']:
        insert_bigv(z)
