# encoding: utf-8
import math
import re

import jieba
from sqlalchemy import create_engine
import pandas as pd
import jieba.analyse as anls


# 加载停词表
def stopwords_list(filepath):
    stopwords = [line.strip().decode('utf-8') for line in open(filepath, 'rb').readlines()]
    return stopwords


# 加载否词表
def not_words_list(filepath):
    not_words = [line.strip().decode('utf-8') for line in open(filepath, 'rb').readlines()]
    return not_words


def pos_words_list(filepath):
    pos_words = [line.decode('utf-8').split(' ')[0] for line in open(filepath, 'rb').readlines()]
    # print(pos_words)
    return pos_words


def neg_words_list(filepath):
    neg_words = [line.decode('utf-8').split(' ')[0] for line in open(filepath, 'rb').readlines()]
    # print(neg_words)
    return neg_words


# 初步筛选分词
def get_seg_sentence(word):
    # print(word)
    # print(len(word))
    # if len(word) == 1:
    #     pass
    if len(word) == 0:
        pass

    else:
        if word not in stopwords_list('stop_words.txt'):
            return word.strip()


# 单句分词
def fenci(text):
    jieba.load_userdict("Internet.txt")
    jieba.load_userdict("comp.txt")
    jieba.load_userdict("neg.txt")
    jieba.load_userdict("pos.txt")
    jieba.load_userdict("not_words.txt")
    jieba.load_userdict("/home/dev2/project/louis/THUOCL_caijing.txt")
    dr = re.compile(r'//@[^:]+:|回复@[^:]+:|\$.*?\$', re.S)
    text1 = dr.sub(' ', text).replace('网页链接','').replace('查看图片','')
    print(text1)
    seg_list = jieba.cut(text1, cut_all=False)
    # print('/'.join(seg_list))
    seg_sentence = []
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
    jieba.load_userdict("/home/dev2/project/louis/THUOCL_caijing.txt")
    jieba.load_userdict("/home/dev2/project/louis/comp.txt")
    jieba.load_userdict("/home/dev2/project/louis/neg.txt")
    jieba.load_userdict("/home/dev2/project/louis/pos.txt")
    jieba.load_userdict('/home/dev2/project/louis/not_words.txt')
    jieba.load_userdict("stop_words")
    word_list = []
    jieba.analyse.set_stop_words('stop_words.txt')
    tags_list = anls.extract_tags(text, 100, allowPOS=('l', 'k'), withFlag=True)
    # 写入list
    for w in tags_list:
        word_list.append([w.word, w.flag])
    print("done!")
    return word_list


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


def get_motion_index_alternative(seg_list):
    pos = 0
    neg = 0
    LAST_IS_NOT = False
    pos_words = pos_words_list('pos.txt')
    neg_words = neg_words_list('neg.txt')
    not_words = not_words_list('not_words.txt')
    print(seg_list)
    print('尽如人意' in pos_words)
    for sag in seg_list:
        print(sag)
        if LAST_IS_NOT:
            if sag in pos_words:
                neg = neg + 1
                print('not pos')
            elif sag in neg_words:
                pos = pos + 1
                print('not neg')
        else:
            if sag in pos_words:
                pos = pos + 1
                print('pos')
            elif sag in neg_words:
                neg = neg + 1
                print('neg')
        if sag in not_words:
            LAST_IS_NOT = True
        else:
            LAST_IS_NOT = False
    motion = math.log((1 + pos) / (1 + neg))
    print(pos)
    print(neg)
    print(motion)
    return motion


if __name__ == '__main__':
    text = """转：科创板 疯狂减持暴跌将来临 
①西部超导：拟减持不超过14%； 
②新光光电：拟减持不超过1.49%； 
③光峰科技：拟减持不超过9%； 
④沃尔德：拟减持不超过9.08%； 
⑤瀚川智能：拟减持不超过3%； 
⑥乐鑫科技：你减持不超过3%；  
未完待续"""
    # print(get_keywords(text))
    # print(get_motion_index(get_keywords(text)))
    get_motion_index_alternative(fenci(text))
