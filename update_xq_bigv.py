import json
import psycopg2
import requests
from copyheaders import headers_raw_to_dict

"""since the bigv_list is not large, it's unlikely to raise an error.
   if error happened, use try and except, which can make sure that program will continue and update entirely.
   e.g. 
   try:
       ...
   except:
       update_bigv_data(userinfo)
   commit()
   """


def get_bigv_data(uid):
    # store userinfo to update
    userinfo = {}
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
    headers = headers_raw_to_dict(headers_raw)
    cookies = {
        'Cookie': 'aliyungf_tc=AQAAAJ4kVxrOCwgAxmMR2pr3vgx12DhM; snbim_minify=true; '
                  'device_id=24700f9f1986800ab4fcc880530dd0ed; Hm_lvt_1db88642e346389874251b5a1eded6e3=1592793404; '
                  's=cj137q59ip; __utmc=1; __utmz=1.1592793568.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); remember=1; '
                  'xq_is_login=1; u=2764635162; bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; '
                  'captcha_id=QwFaqtJ13sksgsp9RatEnEOgh7qJXn; xq_a_token=84f14d8d58bc742ddbdfb3443523553d63a8025b; '
                  'xqat=84f14d8d58bc742ddbdfb3443523553d63a8025b; xq_r_token=9b4d7ef924beea602bac78e7eb97363f6f5edbeb; '
                  'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9'
                  '.eyJ1aWQiOjI3NjQ2MzUxNjIsImlzcyI6InVjIiwiZXhwIjoxNTk1MzkwNTM3LCJjdG0iOjE1OTI3OTg1MzcyNjQsImNpZCI6ImQ5ZDBuNEFadXAifQ.kVnwLdzuHrBpSuRQczcC13mNjqKGx6yUtnisXKaKcrrz_VNcFuZEAo7MlMBCDydipOJYmcVy2n45r3uoefagrOqPEAvHCBVjzjMNPZiALu42wbbfN9zrG7DsOAVnd8zlHidZLf0E7dNP1w_RtTKP7KY8nAcUFNWLYawTnGc3D75v7vW4nE2orUlFNPzm1DAaXeGG7e2LrNzgTWktIydCD_wxRvPuDd7ABF54xJKE-jljK0cAPriyvqztE09JvBW_Q7xvQm2sxnpr19xzWrU01tTPW4fEEKfw1xYSJnS3ePqF2X5GdJ1TzJHJXesUPoF6nYOdwLqDrDhSlJpyId_AKg; __utma=1.817981092.1592793568.1592798313.1592807818.3; acw_tc=2760825415928125915283497ecb0589c794e8b03b21c83f2bf712a19fe0a2; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1592812751 '
    }
    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    dic = json.loads(r.text)
    # print(dic)
    # print(dic['statuses'])
    userinfo['uid'] = uid
    # if not dic['statuses']:
    #     return []
    # print "alter"
    # url_alter = " https://xueqiu.com/statuses/original/show.json?user_id=%s".format(uid)
    # r_al = requests.get(url_alter, headers=headers, cookies=cookies,
    #                     allow_redirects=False, timeout=None)
    # dic_alter = r_al.text
    # print (dic_alter)
    # k_al = dic_alter["user"]
    # userinfo['province'] = k_al['province']
    # userinfo['city'] = k_al['city']
    # userinfo['gender'] = k_al['gender']
    # userinfo['friends_count'] = k_al['friends_count']
    # userinfo['stocks_count'] = k_al['stocks_count']
    # userinfo['description'] = k_al['description']
    # userinfo['status_count'] = k_al['status_count']
    # userinfo['verified_description'] = k_al['verified_description']
    # userinfo['verified_type'] = k_al['verified_type']
    # userinfo['screen_name'] = k_al['screen_name']
    # userinfo['verified'] = k_al['verified']
    # userinfo['followers_count'] = k_al['followers_count']
    # userinfo['donate_count'] = k_al['donate_count']
    if dic['statuses']:
        k = dic['statuses'][0]['user']
        # write userinfo into list
        userinfo['province'] = k['province']
        userinfo['city'] = k['city']
        userinfo['gender'] = k['gender']
        userinfo['friends_count'] = k['friends_count']
        userinfo['stocks_count'] = k['stocks_count']
        userinfo['description'] = k['description']
        userinfo['status_count'] = k['status_count']
        userinfo['verified_description'] = k['verified_description']
        userinfo['verified_type'] = k['verified_type']
        userinfo['screen_name'] = k['screen_name']
        userinfo['verified'] = k['verified']
        userinfo['followers_count'] = k['followers_count']
        userinfo['donate_count'] = k['donate_count']
    else:
        # return {}
        url_alter = """https://xueqiu.com/statuses/original/show.json?user_id={}""".format(uid)
        r_alter = requests.get(url_alter, headers=headers, cookies=cookies,
                               allow_redirects=False, timeout=None)
        dic_alter = json.loads(r_alter.text)
        if dic_alter['user']:
            userinfo['province'] = dic_alter['user']['province']
            userinfo['city'] = dic_alter['user']['city']
            userinfo['gender'] = dic_alter['user']['gender']
            userinfo['friends_count'] = dic_alter['user']['friends_count']
            userinfo['stocks_count'] = dic_alter['user']['stocks_count']
            userinfo['description'] = dic_alter['user']['description']
            userinfo['status_count'] = dic_alter['user']['status_count']
            userinfo['verified_description'] = dic_alter['user']['verified_description']
            userinfo['verified_type'] = dic_alter['user']['verified_type']
            userinfo['screen_name'] = dic_alter['user']['screen_name']
            userinfo['verified'] = dic_alter['user']['verified']
            userinfo['followers_count'] = dic_alter['user']['followers_count']
            userinfo['donate_count'] = dic_alter['user']['donate_count']
        else:
            return {}
    # maxPage
    url_page = "https://xueqiu.com/friendships/followers.json?uid={}&pageNo=1".format(uid)
    r = requests.get(url_page, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    dictstr = json.loads(r.text)
    userinfo['maxPage'] = dictstr['maxPage']
    # print(userinfo)
    return userinfo


def update_bigv_data(userinfo):
    conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    # print("Opened database successfully")
    cur = conn.cursor()
    sql = """update xq_bigv set  
    province= %s, city= %s, gender= %s, friends_count= %s, stocks_count= %s, description= %s, status_count= %s, 
    verified_description= %s, verified_type= %s, screen_name= %s, verified= %s, followers_count= %s, donate_count= %s,
    "maxPage"= %s, updated_at= NOW() where uid = %s  """
    params = (
        userinfo['province'], userinfo['city'], userinfo['gender'], userinfo['friends_count'], userinfo['stocks_count'],
        userinfo['description'], userinfo['status_count'], userinfo['verified_description'], userinfo['verified_type'],
        userinfo['screen_name'], userinfo['verified'], userinfo['followers_count'], userinfo['donate_count'],
        userinfo['maxPage'], userinfo['uid'],)
    # print(params)
    cur.execute(sql, params, )
    # commit
    conn.commit()
    print(userinfo['uid'] + "Operation done successfully")
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
    # print("rows")
    # print(rows)
    return rows


if __name__ == "__main__":
    # print(get_bigv_data('8512175878'))
    for row in get_uid():
        if get_bigv_data(row[0]):
            update_bigv_data(get_bigv_data(row[0]))
