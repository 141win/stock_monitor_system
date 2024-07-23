# _*_ coding: UTF-8 _*_
# @Time : 2024/6/27 12:19
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : function3.py
# @Project : stockSpiderAndstockMonitor
from datetime import timedelta
from record_sendemail_state import send_email_and_record
import pandas as pd

"""
使用到的目录有
send_email_data
amplitude
fall_and_amplitude_over21
"""


def func3_send_email():
    try:
        df = pd.read_csv('../send_email_data/func3.csv')
        if df.shape[0] > 0:
            s = str(df.shape[0]) + '支股票异常：最近7天中某天股价下跌且振荡幅度超过21%，今天也下跌，收盘价小于等于振荡当天的收盘价的95%\n'
            s = s + "{0:<10}{1:<8}{2:<8}{3:<}\n".format('名称', '今日收盘价', '振荡收盘价', '振荡日期')
            rows_as_lists = []
            for index, row in (df.iterrows()):
                rows_as_lists.append(list(row))
            for j in range(0, len(rows_as_lists)):
                t = "{0:<10}{1:<8.2f}{2:<8.2f}{3:<}\n".format(rows_as_lists[j][0], rows_as_lists[j][1],
                                                              rows_as_lists[j][2], rows_as_lists[j][3])
                s = s + t
            send_email_and_record(s, 3, '异常3')
    except Exception as e:
        print("func3_send_email{}", e)


def function3(today_date):
    """
    :param today_date:当前日期（振荡当天的，启用时间是2024-06-06 00：00，那么日期应该是2024-06-05）
    :return: 返回发送邮件的内容
    """
    if today_date.hour == 8:
        name_max_min_open_closing_amplitude_percentage = pd.read_csv(
            f"../amplitude/{today_date.strftime("%Y-%m-%d")} 8.csv")
    else:
        name_max_min_open_closing_amplitude_percentage = pd.read_csv(
            f"../amplitude/{today_date.strftime("%Y-%m-%d")}.csv")
    # 找出今天跌的股票
    name_max_min_open_closing_amplitude_percentage = name_max_min_open_closing_amplitude_percentage[
        name_max_min_open_closing_amplitude_percentage['open_price'] > name_max_min_open_closing_amplitude_percentage[
            'closing_price']]
    # 单独存储今天跌的股票的name、closing_price
    today_fall = name_max_min_open_closing_amplitude_percentage.iloc[:, [0, 4]].copy()
    # 找出今天振荡幅度大于等于21%的股票
    name_max_min_open_closing_amplitude_percentage = name_max_min_open_closing_amplitude_percentage[
        name_max_min_open_closing_amplitude_percentage['amplitude_percentage'] >= 21]
    name_max_min_open_closing_amplitude_percentage = name_max_min_open_closing_amplitude_percentage.drop(
        columns=['max_price', 'min_price', 'open_price', 'amplitude_percentage']
    )
    # 还剩name、closing_price 2列
    today_date_str = today_date.strftime("%Y-%m-%d")
    if today_date.hour == 8:
        name_max_min_open_closing_amplitude_percentage.to_csv(
            f"../fall_and_amplitude_over21/{today_date.strftime("%Y-%m-%d")} 8.csv", index=False)
    else:
        name_max_min_open_closing_amplitude_percentage.to_csv(
            f"../fall_and_amplitude_over21/{today_date.strftime("%Y-%m-%d")}.csv", index=False)
    df = pd.DataFrame()
    for i in range(1, 7):
        monitor_day = today_date - timedelta(days=i)
        try:
            if monitor_day.hour == 8:
                amplitude_over21_df = pd.read_csv(
                    f"../fall_and_amplitude_over21/{monitor_day.strftime("%Y-%m-%d")} 8.csv")
            else:
                amplitude_over21_df = pd.read_csv(
                    f"../fall_and_amplitude_over21/{monitor_day.strftime("%Y-%m-%d")}.csv")
        except:
            continue
        num = amplitude_over21_df.shape[0]
        if num == 0:
            continue
        else:
            match_df = pd.merge(today_fall, amplitude_over21_df, on='name',
                                how='left')
            match_df = match_df[match_df.iloc[:, 1] <= match_df.iloc[:, 2] * 0.95]
            num = match_df.shape[0]
            if num > 0:
                match_df['monitor_day_str'] = monitor_day.strftime("%Y-%m-%d")
                df = pd.concat([df, match_df], ignore_index=True)
    df.to_csv("../send_email_data/func3.csv", index=False)
    # 发邮件
    func3_send_email()
