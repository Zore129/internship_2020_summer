# coding=utf-8
import csv
import json
import random

import psycopg2
import requests
from copyheaders import headers_raw_to_dict
import pandas as pd


# 使用时，注意改变uid
def getData(uid):
    url = "https://xueqiu.com/friendships/followers.json?uid={}&pageNo=1".format(uid)

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
        'Cookie': """device_id=24700f9f1986800ab4fcc880530dd0ed; s=cj137q59ip; __utmz=1.1592793568.1.1.utmcsr=(
        direct)|utmccn=(direct)|utmcmd=(none); remember=1; xq_is_login=1; u=2764635162; 
        bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; xq_a_token=84f14d8d58bc742ddbdfb3443523553d63a8025b; 
        xqat=84f14d8d58bc742ddbdfb3443523553d63a8025b; xq_r_token=9b4d7ef924beea602bac78e7eb97363f6f5edbeb; 
        xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9
        .eyJ1aWQiOjI3NjQ2MzUxNjIsImlzcyI6InVjIiwiZXhwIjoxNTk1MzkwNTM3LCJjdG0iOjE1OTI3OTg1MzcyNjQsImNpZCI6ImQ5ZDBuNEFadXAifQ.kVnwLdzuHrBpSuRQczcC13mNjqKGx6yUtnisXKaKcrrz_VNcFuZEAo7MlMBCDydipOJYmcVy2n45r3uoefagrOqPEAvHCBVjzjMNPZiALu42wbbfN9zrG7DsOAVnd8zlHidZLf0E7dNP1w_RtTKP7KY8nAcUFNWLYawTnGc3D75v7vW4nE2orUlFNPzm1DAaXeGG7e2LrNzgTWktIydCD_wxRvPuDd7ABF54xJKE-jljK0cAPriyvqztE09JvBW_Q7xvQm2sxnpr19xzWrU01tTPW4fEEKfw1xYSJnS3ePqF2X5GdJ1TzJHJXesUPoF6nYOdwLqDrDhSlJpyId_AKg; __utma=1.817981092.1592793568.1592907319.1594089109.8; aliyungf_tc=AQAAAESgE36qhQIAxmMR2kz/d5zJbERn; acw_tc=2760821915941794406714465e985f2840f0844a02051f82a8d53797e9a7a6; Hm_lvt_1db88642e346389874251b5a1eded6e3=1594120113,1594172943,1594179323,1594179929; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1594179949"""}# s = requests.session()
    headers = headers_raw_to_dict(headers_raw)
    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    print(r.text)
    dictstr = json.loads(r.text)
    maxPage = dictstr['maxPage']
    print(maxPage)

    # 查询followers的字段
    # for i in dictstr['followers'][0]:
    #     print(i)
    #     print(dictstr['followers'][0][i])
    #     print()

    print(dictstr['followers'][0])

    for i in sampling(maxPage):
        print(i)
        url = "https://xueqiu.com/friendships/followers.json?uid={}&pageNo={}".format(uid, i + 1)
        # print(url)
        r = requests.get(url, headers=headers, cookies=cookies,
                         allow_redirects=False, timeout=None)
        dic = json.loads(r.text)
        # print(dic)
        for k in dic['followers']:
            insert_followers(
                [uid, k['id'], k['screen_name'], k['stocks_count'], k['followers_count'], k['friends_count'],
                 k['status_count']])
        print(str(i) + '____done')
        # except:
        #     print('error!')
        #     print(i)
        #     url = "https://xueqiu.com/friendships/followers.json?uid={}&pageNo={}".format(uid, i + 1)
        #     # print(url)
        #     r = requests.get(url, headers=headers, cookies=cookies,
        #                      allow_redirects=False, timeout=None)
        #     dic = json.loads(r.text)
        #     for k in dic['followers']:
        #         csv_writer.writerow(
        #             [uid, k['id'], k['screen_name'], k['stocks_count'], k['followers_count'], k['friends_count'],
        #              k['status_count']])
        #     print(str(i) + '____done')

    pass


def get_bigv_number():
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    sql = """SELECT count(uid) FROM xq_bigv"""
    cur.execute(sql)
    row = cur.fetchone()
    # notice the structure of list as below
    # print(rows[0][0])
    return row[0]


def insert_followers(uid, fid, screen_name, stocks_count, followers_count, friends_count,
                     status_count):
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    sql = """insert into xq_followers_sample_copy1(uid,fid,screen_name,stocks_count,followers_count,friends_count,
    status_count) values (%s,%s,%s,%s,%s,%s,%s) """
    params = (uid, fid, screen_name, stocks_count, followers_count, friends_count,
              status_count,)
    # print(params)
    cur.execute(sql, params, )
    # commit
    conn.commit()


def sampling(maxPage):
    sample_list = []
    if maxPage < 250:
        sample_list = range(maxPage)
    else:
        sample_list = random.sample(range(maxPage), 250)
        print(sample_list)
    return sample_list


def get_uid():
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    sql = """SELECT uid FROM xq_bigv"""
    cur.execute(sql)
    rows = cur.fetchall()
    # notice the structure of list as below
    # print(rows[0][0])
    return rows


def delete_followers_data():
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    print("Opened database successfully")
    cur = conn.cursor()
    sql = """DELETE FROM xq_followers_sample_copy1"""
    cur.execute(sql)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    delete_followers_data()
    for user_id in get_uid():
        getData(user_id)
