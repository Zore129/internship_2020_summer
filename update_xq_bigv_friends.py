import json
import psycopg2
import requests
from copyheaders import headers_raw_to_dict


def get_bigv_friends_data(uid):
    # store userinfo to update
    url = "https://xueqiu.com/friendships/groups/members.json?uid={}&page=1&gid=0".format(uid)

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
    headers = headers_raw_to_dict(headers_raw)
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
    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    dictstr = json.loads(r.text)
    maxPage = dictstr['maxPage']
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    print("Opened database successfully")
    cur = conn.cursor()
    if maxPage != 0:
        for pagenum in range(maxPage):
            url = "https://xueqiu.com/friendships/groups/members.json?uid={}&page={}&gid=0".format(uid, pagenum + 1)
            r = requests.get(url, headers=headers, cookies=cookies,
                             allow_redirects=False, timeout=None)
            dic = json.loads(r.text)
            for k in dic['users']:
                sql = """insert into xq_bigv_friends(\"from\",\"to\",screen_name,stocks_count,
                followers_count,friends_count, status_count) values(%s,%s,%s,%s,%s,%s,%s) """
                params = (
                    uid, k['id'], k['screen_name'], k['stocks_count'],
                    k['followers_count'], k['friends_count'],
                    k['status_count'])
                # print(params)
                cur.execute(sql, params, )
                # commit
                conn.commit()
            print(str(pagenum) + "____done")
    conn.close()


def get_uid():
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    print("Opened database successfully")
    cur = conn.cursor()
    sql = """SELECT uid FROM xq_bigv"""
    cur.execute(sql)
    rows = cur.fetchall()
    # notice the structure of list as below
    # print(rows[0][0])
    return rows


def delete_friends_data():
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    print("Opened database successfully")
    cur = conn.cursor()
    sql = """DELETE FROM xq_bigv_friends"""
    cur.execute(sql)
    conn.commit()
    conn.close()


# def insert_friends_data(userinfo_list):
#     if userinfo_list:
#         print(userinfo_list)
#         conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
#                                 host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
#         print("Opened database successfully")
#         cur = conn.cursor()
#         for userinfo in userinfo_list:
#             sql = """insert into xq_bigv_friends_copy1(\"from\",\"to\",screen_name,stocks_count,followers_count,friends_count,
#             status_count) values(%s,%s,%s,%s,%s,%s,%s) """
#             print(userinfo)
#             params = (
#                 userinfo['from'], userinfo['to'], userinfo['screen_name'], userinfo['stocks_count'],
#                 userinfo['followers_count'], userinfo['friends_count'],
#                 userinfo['status_count'])
#             # print(params)
#             cur.execute(sql, params, )
#             # commit
#             conn.commit()
#             print("Operation done successfully")
#         conn.close()


if __name__ == "__main__":
    delete_friends_data()
    for row in get_uid():
        get_bigv_friends_data(row[0])
