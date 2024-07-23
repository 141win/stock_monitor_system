# _*_ coding: UTF-8 _*_
# @Time : 2024/6/27 16:09
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : function6.py
# @Project : stockSpiderAndstockMonitor
from datetime import timedelta
import pandas as pd
from record_sendemail_state import send_email_and_record

"""
使用到的目录有
one_hour_rise_fall
one_hour_rise_0.9
once_rise_fall
twice_rise_fall
thrice_rise_fall
"""


def save_one_hour_rise(rise_stock, now_time):
    # 保存name和涨前价
    rise_stock = rise_stock.iloc[:, [0, 1]].copy()
    rise_stock.to_csv(f'../one_hour_rise_0.9/{now_time.strftime('%Y-%m-%d %H')}.csv',
                      index=False)


def once_rise_fall(now_time, fall_name_now_price):
    df = pd.DataFrame()
    for i in range(1, 7):
        time = now_time - timedelta(hours=i)
        try:
            df1 = pd.read_csv(f'../one_hour_rise_0.9/{time.strftime('%Y-%m-%d %H')}.csv')
        except:
            continue
        df1.columns = ['name', 'rise_price']
        df1 = pd.merge(df1, fall_name_now_price, on='name', how='inner')
        # 添加涨的时间
        df1['rise_time'] = time.strftime('%Y-%m-%d %H')
        if df1.shape[0] > 0:
            # 将涨前价和跌后价比，选取跌后价比涨前价低的股票
            df1 = df1[df1.iloc[:, 1] > df1.iloc[:, 2]]
            # 将df1添加到df中
            df = pd.concat([df, df1], ignore_index=True)
    # 存入一次涨跌异常表
    df = df.drop(columns=df.columns[2])
    # 还剩name、rise_price、rise_time
    df.to_csv(f'../once_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv', index=False)


def twice_rise_fall(now_time):
    df = pd.DataFrame()
    try:
        one_time = pd.read_csv(f'../once_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv')
        # 删除第二次的涨前价
        # 还剩name和rise_time
        one_time = one_time.drop(columns=['rise_price'])
    except:
        return
    for i in range(2, 8):
        time = now_time - timedelta(hours=i)
        try:
            df1 = pd.read_csv(f'../once_rise_fall/{time.strftime('%Y-%m-%d %H')}.csv')
        except:
            continue
        df1.columns = ['name', 'once_rise_price', 'once_rise_time']
        df1 = pd.merge(df1, one_time, on='name', how='inner')
        df1 = df1[time.strftime('%Y-%m-%d %H') < df1['rise_time']]
        if df1.shape[0] > 0:
            df1 = df1.drop(columns=['rise_time'])
            df = pd.concat([df, df1], ignore_index=True)
    # 还剩name、once_rise_time、once_rise_price
    df.to_csv(f'../twice_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv', index=False)


def thrice_rise_fall(now_time):
    df = pd.DataFrame()
    try:
        once = pd.read_csv(f'../once_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv')
    except:
        return
    for i in range(2, 8):
        time = now_time - timedelta(hours=i)
        try:
            df1 = pd.read_csv(f'../twice_rise_fall/{time.strftime('%Y-%m-%d %H')}.csv')
        except:
            continue
        df1 = pd.merge(df1, once, on='name', how='inner')
        df1 = df1[time.strftime('%Y-%m-%d %H') < df1['rise_time']]
        df1 = df1[df1['rise_price'] < df1['once_rise_price']]
        if df1.shape[0] > 0:
            df = pd.concat([df, df1], ignore_index=True)
    # 保存3次涨跌的股票
    df.to_csv(f'../thrice_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv', index=False)


def func6_send_email(now_time):
    try:
        df = pd.read_csv(f'../thrice_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv')
        if df.shape[0] > 0:
            s = str(df.shape[0]) + "支股票异常：3次涨跌，且最后一次涨前价小于第一次的涨前价\n"
            s = s + "{0:<10}{1:<15}{2:<10}\n".format('名称', '第一次涨时间', '第三次涨时间')
            df = df.drop(columns=['once_rise_price', 'rise_price'])
            # 将df按照名字排序
            df = df.sort_values(by=['name'])
            # 发送邮件
            # 此时还有name、once_rise_time、rise_time
            rows_as_lists = []
            for index, row in df.iterrows():
                rows_as_lists.append(list(row))
            for j in range(0, len(rows_as_lists)):
                # 将每一行转换为字符串时，保证每一列对齐
                t = "{0:<10}{1:<15}{2:<10}\n".format(rows_as_lists[j][0], rows_as_lists[j][1], rows_as_lists[j][2])
                s = s + t
            send_email_and_record(s, 6, "异常6")
            # print(s)
    except Exception as e:
        print("func6_send_email{}", e)


def function6(now_time):
    # 读取涨跌幅及价格
    rise_fall_percentage = pd.read_csv(f'../one_hour_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv')
    # 选择涨0.9%的股票
    rise_stock = rise_fall_percentage[rise_fall_percentage['rise_fall_percentage'] >= 0.9].copy()
    # 保存涨0.9%的股票
    save_one_hour_rise(rise_stock, now_time)

    # 获取当前跌的股票
    # 提取name和price2两列
    fall_name_now_price = rise_fall_percentage[rise_fall_percentage['rise_fall_percentage'] < 0].iloc[:, [0, 2]].copy()
    once_rise_fall(now_time, fall_name_now_price)
    twice_rise_fall(now_time)
    thrice_rise_fall(now_time)
    func6_send_email(now_time)

