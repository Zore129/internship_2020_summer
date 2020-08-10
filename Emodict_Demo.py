# encoding: utf-8
import logging
import math
import os
import re
import sys
from time import sleep

import jieba
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import jieba.analyse as anls


def log():
    filename_log = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:] + ".log"
    logging.basicConfig(filename=filename_log, level=logging.DEBUG, format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


# 加载停词表
def stopwords_list(filepath):
    stopwords = [line.strip().decode('utf-8') for line in open(filepath, 'rb').readlines()]
    return stopwords


# 加载否词表
def not_words_list(filepath):
    not_words = [line.strip().decode('utf-8') for line in open(filepath, 'rb').readlines()]
    return not_words


# 加载正面词表
def pos_words_list(filepath):
    pos_words = [line.decode('utf-8').split(' ')[0] for line in open(filepath, 'rb').readlines()]
    # print(pos_words)
    return pos_words


# 加载负面词表
def neg_words_list(filepath):
    neg_words = [line.decode('utf-8').split(' ')[0] for line in open(filepath, 'rb').readlines()]
    # print(neg_words)
    return neg_words


# 用停词表初步筛选分词
def get_seg_sentence(word):
    # print(word)
    # print(len(word))
    # if len(word) == 1:
    #     pass
    if len(word) == 0:
        pass
    else:
        if word not in stopwords_list('/home/dev2/project/louis/stop_words.txt'):
            return word.strip()


# 单句分词
def fenci(text):
    seg_sentence = []
    jieba.load_userdict("/home/dev2/project/louis/Internet.txt")
    jieba.load_userdict("/home/dev2/project/louis/comp.txt")
    jieba.load_userdict("/home/dev2/project/louis/neg.txt")
    jieba.load_userdict("/home/dev2/project/louis/pos.txt")
    jieba.load_userdict("/home/dev2/project/louis/not_words.txt")
    # 去掉回复@XXX:和//@XXXX:和$(股票名代码)$【可以在stockcorrelation中找到】
    dr = re.compile(r'//@[^:]+:|回复@[^:]+:|\$.*?\$', re.S)
    # 去掉网页链接和查看图片
    if text:
        text = dr.sub(' ', text).replace('网页链接', '').replace('查看图片', '')
        # print(text1)
        seg_list = jieba.cut(text, cut_all=False)
        # print('/'.join(seg_list))
        # 转存list
        # print(seg_list)
        for word in seg_list:
            if get_seg_sentence(word):
                seg_sentence.append(get_seg_sentence(word))
    return seg_sentence


def get_keywords(text):
    """当进行增量更新的时候，写入的时候用这个方法，对文本进行处理，在另一张表中增加关键词信息。
    要保证处理的文本之前没有处理入表过。也就是这个要在最后写入xq_value表时候操作，要先做增量比对。
    最重要的是在anls.extract_tags(文本, 20, allowPOS=('n', 'a'), withFlag=True)
    语句中str类型文本的输入，怎么得到文本可以用其他的方法
    例如：用该方法直接传入text，anls.extract_tags(text, 20, allowPOS=('n', 'a'), withFlag=True)"""
    not_words = not_words_list('/home/dev2/project/louis/not_words.txt')
    dr1 = re.compile(r'//@[^:]+:|回复@[^:]+:|\$.*?\$', re.S)
    # 去掉网页链接和查看图片
    if text:
        text = dr1.sub(' ', text).replace('网页链接', '').replace('查看图片', '')
        dr2 = re.compile(r'[0-9]+', re.S)
        if text:
            text2 = dr2.sub('', text)
    # print(text2)
    jieba.load_userdict("/home/dev2/project/louis/Internet.txt")
    jieba.load_userdict("/home/dev2/project/louis/comp.txt")
    jieba.load_userdict("/home/dev2/project/louis/neg.txt")
    jieba.load_userdict("/home/dev2/project/louis/pos.txt")
    jieba.load_userdict("/home/dev2/project/louis/not_words.txt")
    word_list = []
    jieba.analyse.set_stop_words('/home/dev2/project/louis/stop_words.txt')
    if text:
        tags_list = anls.extract_tags(text, 30, withFlag=True)
        # 写入list
        for w in tags_list:
            if w not in not_words:
                word_list.append(w)
        print("done!")
    return word_list


def get_statuses_id():
    conn = psycopg2.connect(database="demo", user="dev1_db", password="HdGY7MHZ6*v2",
                            host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    cur = conn.cursor()
    cur.execute("""set search_path to xueqiu""")
    sql = """select id,processed_text from xueqiu.xq_bigv_statuses where keyword_flag=0"""
    rows = pd.read_sql(sql=sql, con=conn)
    print(rows)
    return rows


def write_keywords(id, processed_text):
    print(id)
    id_list = []
    word_list = []
    index_list = []
    tags_list = get_keywords(processed_text)
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /demo""")
    for w in tags_list:
        index_list.append(tags_list.index(w) + 1)
        id_list.append(id)
        word_list.append(w)
    df = pd.DataFrame({'id': id_list, 'word': word_list, 'index': index_list})
    pd.io.sql.to_sql(df, 'xq_keywords', engine, schema='xueqiu', index=False, if_exists='append')


# 写入数据库
def write_keywords_main():
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /demo""")
    for index, row in get_statuses_id().iterrows():
        sleep(1)
        write_keywords(row[0], row[1])
        sql1 = """update "xueqiu".xq_bigv_statuses set keyword_flag=1 where id=%s"""
        params = (row[0],)
        engine.execute(sql1, params, )


# 用关键词得出情绪
def get_motion_index(tag_list):
    pos = 0
    neg = 0
    for tag in tag_list:
        print(tag[1])
        if tag[1] == 'l':
            pos = pos + 1
        if tag[1] == 'k':
            neg = neg + 1
    motion = math.log((1 + pos) / (1 + neg))
    print(pos)
    print(neg)
    return motion


# 自己分词得到情绪
def get_motion_index_alternative(seg_list):
    pos = 0
    neg = 0
    LAST_IS_NOT = False
    pos_words = pos_words_list('/home/dev2/project/louis/pos.txt')
    neg_words = neg_words_list('/home/dev2/project/louis/neg.txt')
    not_words = not_words_list('/home/dev2/project/louis/not_words.txt')
    # print(seg_list)
    for sag in seg_list:
        # print(sag)
        if LAST_IS_NOT:
            if sag in pos_words:
                neg = neg + 1
                # print('not pos')
            elif sag in neg_words:
                pos = pos + 1
                # print('not neg')
        else:
            if sag in pos_words:
                pos = pos + 1
                # print('pos')
            elif sag in neg_words:
                neg = neg + 1
                # print('neg')
        # 如果上一个是否定词，下一个词性相反
        if sag in not_words:
            LAST_IS_NOT = True
        else:
            LAST_IS_NOT = False
    motion = math.log((1 + pos) / (1 + neg))
    # print(pos)
    # print(neg)
    print(motion)
    return motion


# 更新数据库
def get_motion_main():
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /demo""")
    sql = """select id,processed_text,sentiments from "xueqiu".xq_bigv_statuses where sentiments=100"""
    rows = pd.read_sql(sql=sql, con=engine)
    for index, row in rows.iterrows():
        sleep(1)
        motion = get_motion_index_alternative(fenci(row[1]))
        sql1 = """update "xueqiu".xq_bigv_statuses set sentiments=%s where id=%s"""
        params = (motion, row[0])
        engine.execute(sql1, params, )


if __name__ == '__main__':
    #     # print(get_keywords(text))
    #     # print(get_motion_index(get_keywords(text)))
    #     # get_motion_index_alternative(fenci(text))
    #     # get_keyword_alternative(fenci(text))
    log()
    logging.info("running.")
    print("running.")
    get_motion_main()
    write_keywords_main()
    logging.info("finish.")
    print("finish.")
