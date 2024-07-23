# _*_ coding: UTF-8 _*_
# @Time : 2024/5/11 23:47
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : read_write_csv.py
# @Project : stockSpiderAndstockMonitor

import pandas as pd


# 从csv中读取数据
def read_data_from_csv(csv_path):
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        df = None
    return df


# 拼接数据并填充缺失值删除重复行
def handler_data(new_df, old_df=None):
    if old_df is None:
        return new_df
    else:
        # 拼接新老数据
        old_df = pd.merge(old_df, new_df, on='name', how='left')
        # 删除重复行
        old_df = old_df.drop_duplicates(subset=['name'], keep='first')
        # 将缺失值用同行的前一列的数据进行补齐
        old_df = old_df.T.bfill().ffill().T
        return old_df


# 将股票数据存入表中
def write_all_stock_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
