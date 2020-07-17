# coding=utf-8
import psycopg2
import pandas as pd
from numpy import nan
from sqlalchemy import create_engine


def read():
    v_ID_list = []
    symbol_list = []
    yr_list = []
    v_typeID_list = []
    is_v_list = []
    df = pd.read_csv('STK_Violation_Main1.csv')
    # print df
    # 用发布年份填充违约年
    for index, row in df.iterrows():
        # print row['ViolationYear']
        if row['ViolationYear'] is nan:
            row['ViolationYear'] = row['DisposalDate'][0:4]
            # print row['ViolationYear']
    # 拆分违约年
    for index, row in df.iterrows():
        yrs = row['ViolationYear'].split(',')
        vids = row['ViolationTypeID'].split(',')
        print yrs, vids
        for y in yrs:
            for v in vids:
                v_ID_list.append(row['ViolationID'])
                symbol_list.append(row['Symbol'])
                yr_list.append(y)
                v_typeID_list.append(v)
                is_v_list.append(row['IsViolated'])
    df = pd.DataFrame({'ViolationID': v_ID_list, 'ViolationYear': yr_list, 'Symbol': symbol_list,
                       'ViolationTypeID': v_typeID_list, 'IsViolated': is_v_list})
    engine = create_engine(
        """postgresql+psycopg2://dev1_db:HdGY7MHZ6*v2@pgm-8vb23fi81368zq03lo.pgsql.zhangbei.rds.aliyuncs.com:1433
        /dev1""")
    pd.io.sql.to_sql(df, 'violation', engine, index=False, if_exists='append')


if __name__ == '__main__':
    read()
