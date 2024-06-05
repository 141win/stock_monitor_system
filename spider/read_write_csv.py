# _*_ coding: UTF-8 _*_
# @Time : 2024/5/11 23:47
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : read_write_csv.py
# @Project : stockSpiderAndstockMonitor

import pandas as pd


# 将每支股票的数据分别存入分表
def write_every_stock_to_csv(df, tradetime):
    # 用迭代器将df中的每行存入对应的csv文件中
    for row in df.itertuples():
        name = row.name
        price = row[2]
        new_df = pd.DataFrame({'name': [name], tradetime: [price]})
        try:
            # 读取老数据
            old_df = pd.read_csv('../every_stock_excel/' + name + '.csv')
            # 拼接新老数据
            old_df = pd.merge(old_df, new_df, on='name', how='outer')
        except:
            old_df = new_df
        old_df.to_csv('../every_stock_excel/' + name + '.csv', index=False)


# 将股票数据存入表中
def write_all_stock_to_csv(df, csv_path):
    try:
        # 读取老数据
        old_df = pd.read_csv(csv_path)
        # 拼接新老数据
        # how = 'outer'
        old_df = pd.merge(old_df, df, on='name', how='left')
        # 删除重复行
        old_df = old_df.drop_duplicates(subset=['name'], keep='first')
        # 将缺失值用同行的前一列的数据进行补齐
        old_df = old_df.T.bfill().ffill().T
    except:
        old_df = df
    old_df.to_csv(csv_path, index=False)
    return old_df
