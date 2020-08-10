import json
import random
import time

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
    dic = json.loads(r.text)
    userinfo['uid'] = uid
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
    conn = psycopg2.connect(database="demo", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    cur.execute("""set search_path to xueqiu""")
    sql = """update xq_bigv set  
    province= %s, city= %s, gender= %s, friends_count= %s, stocks_count= %s, description= %s, status_count= %s, 
    verified_description= %s, verified_type= %s, screen_name= %s, verified= %s, followers_count= %s, donate_count= %s,
    "maxPage"= %s, updated_at= NOW() where uid = %s  """
    params = (
        userinfo['province'], userinfo['city'], userinfo['gender'], userinfo['friends_count'], userinfo['stocks_count'],
        userinfo['description'], userinfo['status_count'], userinfo['verified_description'], userinfo['verified_type'],
        userinfo['screen_name'], userinfo['verified'], userinfo['followers_count'], userinfo['donate_count'],
        userinfo['maxPage'], str(userinfo['uid']),)
    # print(params)
    cur.execute(sql, params, )
    # commit
    conn.commit()
    print(str(userinfo['uid']) + "Operation done successfully")
    conn.close()


def insert_bigv_data(userinfo):
    conn = psycopg2.connect(database="demo", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    cur.execute("""set search_path to xueqiu""")
    sql = """insert into xq_bigv(province, city, gender, friends_count, stocks_count, description, status_count, 
    verified_description, verified_type, screen_name, verified, followers_count, donate_count,"maxPage", created_at, 
    updated_at, uid, batch)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(),NOW(), %s, %s) """
    params = (
        userinfo['province'], userinfo['city'], userinfo['gender'], userinfo['friends_count'], userinfo['stocks_count'],
        userinfo['description'], userinfo['status_count'], userinfo['verified_description'], userinfo['verified_type'],
        userinfo['screen_name'], userinfo['verified'], userinfo['followers_count'], userinfo['donate_count'],
        userinfo['maxPage'], str(userinfo['uid']), 2)
    # print(params)
    cur.execute(sql, params, )
    # commit
    conn.commit()
    print(str(userinfo['uid']) + "Operation done successfully")
    conn.close()


def get_uid():
    id_list = []
    conn = psycopg2.connect(database="demo", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    cur.execute("""set search_path to xueqiu""")
    sql = """SELECT uid FROM xq_bigv"""
    cur.execute(sql)
    rows = cur.fetchall()
    for row_id in rows:
        id_list.append(row_id[0])
    return id_list


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
    id_list = get_uid()
    print(id_list)
    for row in get_uid_from_web():
        print(row)
        if str(row) in id_list:
            update_bigv_data(get_bigv_data(row))
        else:
            insert_bigv_data(get_bigv_data(row))
