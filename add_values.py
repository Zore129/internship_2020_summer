# coding=utf-8
"""得到下一个url(称为本页)
上一页的json包里的key中
next_max_id 是上一页的最后一个帖子的id
next_id 是本页的第一个帖子的id
"""
import json

import psycopg2
import requests
from copyheaders import headers_raw_to_dict


def get_next_url(id):
    url = """https://xueqiu.com/v4/statuses/home_timeline.json?source=user&max_id={}""".format(str(id))
    return url


# 将帖子写入数据库
def write_json(begin_id):
    # request json
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
    headers = headers_raw_to_dict(headers_raw)
    # 这里的143600000是断点，意思是断点之后的都存了，而且可能在断点前的页面上一部分也存了，需要手工拼接
    while begin_id > 153600000:
        url = get_next_url(begin_id)
        print(url)
        print(begin_id)
        r = requests.get(url, headers=headers, cookies=cookies,
                         allow_redirects=False, timeout=None)
        dic = json.loads(r.text)
        # dic["home_timeline"][0] 是下一个帖子的信息
        w = dic["home_timeline"][0]
        # begin_id 赋值为下一个帖子的id
        begin_id = int(w['id'])
        # 连接数据库，写入
        conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                                host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
        cur = conn.cursor()
        # 作为调试，只存了2个字段
        sql = """insert into xq_add_value(id,text) values (%s, %s)"""
        params = (w['id'], w['text'])
        cur.execute(sql, params, )
        # commit
        conn.commit()
        print("loop_done")


if __name__ == '__main__':
    begin_id = 153956771
    write_json(begin_id)
