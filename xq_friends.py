import csv
import json
import requests
from copyheaders import headers_raw_to_dict
import pandas as pd

f = open('friends_lost.csv', 'w', encoding='utf-8-sig', newline='')
csv_writer = csv.writer(f)
csv_writer.writerow(
    ['from', 'to', 'screen_name', 'stocks_count', 'followers_count', 'friends_count', 'status_count'])


# 使用时，注意改变uid
def getData(uid):
    datalist = []

    url = "https://xueqiu.com/friendships/groups/members.json?uid={}&page=1&gid=0".format(uid)

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
        X-Requested-With: XMLHttpRequest
        """
    headers = headers_raw_to_dict(headers_raw)
    cookies = {
        'Cookie': 'aliyungf_tc=AQAAAJ4kVxrOCwgAxmMR2pr3vgx12DhM; snbim_minify=true; '
                  'device_id=24700f9f1986800ab4fcc880530dd0ed; Hm_lvt_1db88642e346389874251b5a1eded6e3=1592793404; '
                  's=cj137q59ip; __utmc=1; __utmz=1.1592793568.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); '
                  'remember=1; xq_is_login=1; u=2764635162; bid=a14ed9b410aee955ba8ad6e90da6a7a1_kbpyznno; '
                  'captcha_id=QwFaqtJ13sksgsp9RatEnEOgh7qJXn; xq_a_token=84f14d8d58bc742ddbdfb3443523553d63a8025b; '
                  'xqat=84f14d8d58bc742ddbdfb3443523553d63a8025b; '
                  'xq_r_token=9b4d7ef924beea602bac78e7eb97363f6f5edbeb; '
                  'xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9'
                  '.eyJ1aWQiOjI3NjQ2MzUxNjIsImlzcyI6InVjIiwiZXhwIjoxNTk1MzkwNTM3LCJjdG0iOjE1OTI3OTg1MzcyNjQsImNpZCI6ImQ5ZDBuNEFadXAifQ.kVnwLdzuHrBpSuRQczcC13mNjqKGx6yUtnisXKaKcrrz_VNcFuZEAo7MlMBCDydipOJYmcVy2n45r3uoefagrOqPEAvHCBVjzjMNPZiALu42wbbfN9zrG7DsOAVnd8zlHidZLf0E7dNP1w_RtTKP7KY8nAcUFNWLYawTnGc3D75v7vW4nE2orUlFNPzm1DAaXeGG7e2LrNzgTWktIydCD_wxRvPuDd7ABF54xJKE-jljK0cAPriyvqztE09JvBW_Q7xvQm2sxnpr19xzWrU01tTPW4fEEKfw1xYSJnS3ePqF2X5GdJ1TzJHJXesUPoF6nYOdwLqDrDhSlJpyId_AKg; acw_tc=2760824215929062509188935e0582eddf4b223155c0a87ee860d4b9c1a223; __utma=1.817981092.1592793568.1592893463.1592907319.7; __utmt=1; __utmb=1.1.10.1592907319; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1592907749'}
    s = requests.session()
    headers = headers_raw_to_dict(headers_raw)
    r = requests.get(url, headers=headers, cookies=cookies,
                     allow_redirects=False, timeout=None)
    print(r.text)
    dictstr = json.loads(r.text)
    maxPage = dictstr['maxPage']
    print(maxPage)

    # 查询followers的字段
    # for i in dictstr['users'][1]:
    #     print(i)
    #     print(dictstr['users'][1][i])
    #     print()

    # print(maxPage)
    # print(dictstr['users'][0])

    for i in range(maxPage):
        print(i)
        url = "https://xueqiu.com/friendships/groups/members.json?uid={}&page={}&gid=0".format(uid, i + 1)
        # print(url)
        r = requests.get(url, headers=headers, cookies=cookies,
                         allow_redirects=False, timeout=None)
        dic = json.loads(r.text)
        for k in dic['users']:
            csv_writer.writerow(
                [uid, k['id'], k['screen_name'], k['stocks_count'], k['followers_count'], k['friends_count'],
                 k['status_count']])
        print(str(i) + '____done')

    pass


f_id = pd.read_csv('user_id.csv')
for z in range(147):
    print(z)
    print(f_id.iloc[z, 0])
    getData(str(f_id.iloc[z, 0]))

f.close()
