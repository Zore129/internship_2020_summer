import psycopg2
import pandas as pd
from numpy import nan


def read():
    df = pd.read_csv('STK_Violation_Main1.csv')
    # print df
    for index, row in df.iterrows():
        # print row['ViolationYear']
        if row['ViolationYear'] is nan:
            row['ViolationYear'] = row['DeclareDate'][0:4]
            print row['ViolationYear']


if __name__ == '__main__':
    read()
