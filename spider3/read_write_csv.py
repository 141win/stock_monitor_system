# _*_ coding: UTF-8 _*_
# @Time : 2024/5/11 23:47
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : read_write_csv.py
# @Project : stockSpiderAndstockMonitor
from datetime import timedelta, datetime
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


def write_all_stock_to_csv(df_stock, today, now_minute, now_hour):
    if now_minute == 0 and now_hour == 0:
        # 读取旧数据
        old_all_stock = read_data_from_csv(
            '../all_stock/stock' + (today - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv')
        # 拼接新数据并填补缺失值
        new_all_stock = handler_data(df_stock, old_all_stock)
        # 将数据存入前一天的总表
        new_all_stock.to_csv('../all_stock/stock' + (today - timedelta(days=1)).strftime(
            '%Y-%m-%d') + '.csv', index=False)
        # 获取最新的数据
        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
        # 将新数据存入新的总表中
        df_stock.to_csv('../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv', index=False)
    else:
        # 读取旧数据
        old_all_stock = read_data_from_csv('../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
        # 拼接新数据并填补缺失值
        new_all_stock = handler_data(df_stock, old_all_stock)
        # 获取最新的数据
        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
        # 将新数据存入总表中
        new_all_stock.to_csv('../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv', index=False)
    print("总表存储完成", datetime.now())
    try:
        if now_minute == 0:
            new_all_stock.iloc[:, [0, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1]].to_csv(
                f'../all_stock_hour_price/{today.strftime('%Y-%m-%d %H')}.csv', index=False)
            print("all_stock_hour_price存储完成")
    except Exception as e:
        print(e)
    return new_all_stock, df_stock


# 存入小时表中
def write_all_stock_one_hour_to_csv(df_stock, today, now_hour):
    # 将每支股票存入分表
    if now_hour == 0:
        # 读取旧数据
        old_one_hour_all_stock = read_data_from_csv(
            '../all_stock_one_hour/' + (today - timedelta(days=1)).strftime("%Y-%m-%d") + '.csv')
        # 拼接数据并填补缺失值
        new_one_hour_all_stock = handler_data(df_stock, old_one_hour_all_stock)
        # 将新数据存入表中
        new_one_hour_all_stock.to_csv('../all_stock_one_hour/' + (today - timedelta(days=1)).strftime(
            "%Y-%m-%d") + '.csv', index=False)
        df_stock.to_csv('../all_stock_one_hour/' + today.strftime("%Y-%m-%d") + '.csv', index=False)

    else:
        # 读取旧数据
        old_one_hour_all_stock = read_data_from_csv(
            '../all_stock_one_hour/' + today.strftime("%Y-%m-%d") + '.csv')
        # 拼接数据并填补缺失值
        new_one_hour_all_stock = handler_data(df_stock, old_one_hour_all_stock)
        # 将新数据存入表中
        new_one_hour_all_stock.to_csv('../all_stock_one_hour/' + today.strftime("%Y-%m-%d") + '.csv', index=False)
    # 小时数据存储完成
    print("小时分表存储完成", datetime.now())
    if new_one_hour_all_stock.shape[1] < 3:
        return None
    else:
        return new_one_hour_all_stock.iloc[:, [0, -2, -1]].copy()


# 将收盘价存入收盘价表中,并返回收盘价表中的数据
def write_all_stock_one_day_to_csv(df_stock, today, now_hour=""):
    """

    :param df_stock:
    :param today: 该函数都是在0点启动，传入的today是0点时间，例如现在是2024-06-07 00:00
    :param now_hour: 字符串
    :return:
    """
    yesterday = today - timedelta(days=1)
    # 将昨天的日期格式化为字符串
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    df_stock.rename(columns={today.strftime("%Y-%m-%d %H:%M"): yesterday_str}, inplace=True)
    # 读取旧数据
    closing_all_stock = read_data_from_csv('../closing_price/closing_price' + now_hour + '.csv')
    # 拼接新旧数据
    closing_all_stock = handler_data(df_stock, closing_all_stock)
    # 存入收盘价表中（0点收盘）
    closing_all_stock.to_csv('../closing_price/closing_price' + now_hour + '.csv', index=False)
    return closing_all_stock
