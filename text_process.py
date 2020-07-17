# coding=utf-8
import sys
import csv
import pandas as pd
import re
import jieba
import psycopg2
import jieba.posseg as pseg  # 词性标注
import jieba.analyse as anls  # 关键词提取
from snownlp import SnowNLP
from io import BytesIO as StringIO
from sqlalchemy import create_engine

reload(sys)
sys.setdefaultencoding('utf-8')


# 处理成可读文本，并得到标题
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


# 加载停词表
# def stopwords_list(filepath):
#     stopwords = [line.strip() for line in open(filepath, 'r').readlines()]
#     return stopwords


# # 初步筛选分词
# def get_seg_sentence(word):
#     # print(word)
#     # print(len(word))
#     if len(word) == 1:
#         pass
#     elif len(word) == 0:
#         pass
#     else:
#         if word not in stopwords_list('stop_words.txt'):
#             return word.strip()


# # 单句分词
# def fenci(text):
#     seg_list = jieba.lcut(text, cut_all=True)
#     # print('/'.join(seg_list))
#     seg_sentence = []
#     # print(seg_list)
#     for word in seg_list:
#         if get_seg_sentence(word):
#             seg_sentence.append(get_seg_sentence(word))
#     return seg_sentence


# # 用于得到句子列表
# def get_sentence_list():
#     f = pd.read_csv('xq_processed_text.csv').astype(str)
#     sentence_list = f.iloc[:, 1].tolist()
#     return sentence_list


# # 通过所有句子得到所有分词列表
# def get_total_list(sentence_list):
#     total_list = [fenci(text) for text in sentence_list]
#     # print(total_list)
#     return total_list
#
#
# def get_tf_idf_list():
#     total_list = get_total_list(get_sentence_list())
#     dict = corpora.Dictionary(total_list)
#     # print([dict[d] for d in dict])
#     corpus = [dict.doc2bow(text) for text in total_list]
#     # print(corpus)
#     tf_idf = models.TfidfModel(corpus)
#     corpus_tf_idf = tf_idf[corpus]
#     seg_bag = []
#     for sent in corpus_tf_idf:
#         sent_seg_bag = []
#         for tup in sent:
#             if tup[1] > 0.5:
#                 sent_seg_bag.append([tup[0], dict[tup[0]], tup[1]])
#         seg_bag.append(sent_seg_bag)
#     return seg_bag


# def insert_keywords(text_id, word, word_type):
#     conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
#                             host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
#     cur = conn.cursor()
#     sql = """insert into xq_keywords(id,word,type,system_id) values (%s,%s,%s,default)"""
#     params = (text_id, word, word_type,)
#     # print(params)
#     cur.execute(sql, params, )
#     # commit
#     conn.commit()

# def get_keywords(text):
def get_keywords():
    """当进行增量更新的时候，写入的时候用这个方法，对文本进行处理，在另一张表中增加关键词信息。
    要保证处理的文本之前没有处理入表过。也就是这个要在最后写入xq_value表时候操作，要先做增量比对。
    最重要的是在anls.extract_tags(文本, 20, allowPOS=('n', 'a'), withFlag=True)
    语句中str类型文本的输入，怎么得到文本可以用其他的方法
    例如：用该方法直接传入text，anls.extract_tags(text, 20, allowPOS=('n', 'a'), withFlag=True)"""
    jieba.load_userdict("finance.txt")
    jieba.load_userdict("stock.txt")
    jieba.load_userdict("company.txt")
    jieba.load_userdict("psy.txt")
    jieba.load_userdict("THUOCL_caijing.txt")
    # 用于储存
    id_list = []
    word_list = []
    type_list = []
    index_list = []
    # 从表中获得处理好的文本
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /dev1""")
    sql = """select id,processed_text from xq_value_processed_text"""
    # 得到文本的df
    rows = pd.read_sql(sql=sql, con=engine)
    # index是df的index  row是一行文本信息的list
    # row[0]是id，row[1]是文本内容
    for index, row in rows.iterrows():
        print(row[0])
        # 得到分词的数据
        # 最多20个词，只能是n、a词性，返回词性，如果想要tf-idf的weight 可以加上withWeight=True
        # 得到的数据类型为包里的特殊类型 w.word是分词 w.flag是词性 w.weight是tf-idf得分
        tags_list = anls.extract_tags(str(row[1]), 20, allowPOS='n', withFlag=True)
        # 写入list
        for w in tags_list:
            index_list.append(tags_list.index(w) + 1)
            id_list.append(row[0])
            word_list.append(w.word)
            type_list.append(w.flag)
        print("done!")
        tags_list = anls.extract_tags(str(row[1]), 20, allowPOS='a', withFlag=True)
        # 写入list
        for w in tags_list:
            index_list.append(tags_list.index(w) + 1)
            id_list.append(row[0])
            word_list.append(w.word)
            type_list.append(w.flag)
        print("done!")
    # 生成df
    df = pd.DataFrame({'id': id_list, 'word': word_list, 'type': type_list, 'index': index_list})
    # print(df)
    # 数据库生成表, 如果已经存在就插入行
    df.to_sql('xq_keywords_1', engine, index=False, if_exists='append')


def get_summary_snowNLP():
    """最重要的是在s = SnowNLP(文本)语句中str类型文本的输入，怎么得到文本可以用其他的方法
    在更新的时候，可以将s.summary(limit=1)[0]直接存为一个字段
    这里是直接把所有summary整合在了一个df中"""
    # 储存数据
    id_list = []
    summary_list = []
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /dev1""")
    sql = """select id,processed_text from xq_nlp"""
    rows = pd.read_sql(sql=sql, con=engine)
    for index, row in rows.iterrows():
        if row[1]:
            # 传入文本得到snownlp对象
            s = SnowNLP(row[1])
            id_list.append(row[0])
            try:
                # limit表示摘要的句子数
                summary_list.append(s.summary(limit=1)[0])
            # 如果标题没有意义，summary()方法会返回空，我们在这里用无意义标题代替
            except:
                summary_list.append("no meaningful title")
    # 存到df中
    df1 = pd.DataFrame({'id': id_list, 'summary': summary_list})
    print(df1)
    df1.to_sql('xq_summary', engine, index=False, if_exists='append')

    # output = StringIO()
    # df1.to_csv(output, sep='\t', index=False, header=False)
    # output1 = output.getvalue()
    # conn = psycopg2.connect(database="dev1", user="dev1_db", password="HdGY7MHZ6*v2",
    #                         host="pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com", port="1433")
    # cur = conn.cursor()
    # cur.copy_from(StringIO(output1), 'xq_keywords_copy1')
    # conn.commit()
    # cur.close()
    # conn.close()


def get_motion_snowNLP():
    """最重要的是在s = SnowNLP(文本)语句中str类型文本的输入，怎么得到文本可以用其他的方法
    在更新的时候，也可以将s.sentiments直接存为一个字段
    这里是直接把所有motion整合在了一个df中"""
    # 储存数据
    id_list = []
    motion_list = []
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /dev1""")
    sql = """select id,processed_text from xq_value_processed_text"""
    rows = pd.read_sql(sql=sql, con=engine)
    for index, row in rows.iterrows():
        if row[1]:
            # 传入文本得到snownlp对象
            s = SnowNLP(row[1])
            id_list.append(row[0])
            motion_list.append(s.sentiments)
    # 存到df中
    df1 = pd.DataFrame({'id': id_list, 'motion': motion_list})
    print(df1)
    df1.to_sql('xq_motion', engine, index=False, if_exists='append')


def write_processed_text():
    id_list = []
    processed_text_list = []
    title_list = []
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /dev1""")
    sql = """select id,text from xq_value_processed_text"""
    # 得到文本的df
    rows = pd.read_sql(sql=sql, con=engine)
    # index是df的index  row是一行文本信息的list
    # row[0]是id，row[1]是文本内容
    for index, row in rows.iterrows():
        print(row[0])
        t = text_clean(row[1])
        # 写入list
        id_list.append(row[0])
        processed_text_list.append(t[1])
        title_list.append(t[0])
        print("done!")
    # 生成df
    df = pd.DataFrame({'id': id_list, 'title': title_list, 'processed_text': processed_text_list})
    # print(df)
    # 数据库生成表, 如果已经存在就插入行
    df.to_sql('xq_value_processed_text_copy1', engine, index=False, if_exists='append')


if __name__ == "__main__":
    get_keywords()
