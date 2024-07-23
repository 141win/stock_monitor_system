# _*_ coding: UTF-8 _*_
# @Time : 2024/6/27 12:20
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : function4.py
# @Project : stockSpiderAndstockMonitor
from datetime import timedelta
from record_sendemail_state import send_email_and_record
import pandas as pd

"""
使用到的目录有
send_email_data
amplitude
function4_monitor_stock
"""


def func4_send_email():
    try:
        df = pd.read_csv('../send_email_data/func4.csv')
        if df.shape[0] > 0:
            s = str(df.shape[0]) + '支股票异常：最近7天中某天股价跌幅超过2%、振荡幅度超过16%、最低价小于等于开盘价的92%；今日收盘价小于等于振荡当天的收盘价的92%\n'
            s = s + "{0:<10}{1:<8}{2:<8}{3:<}\n".format('名称', '今日收盘价', '振荡收盘价', '振荡日期')
            rows_as_lists = []
            for index, row in (df.iterrows()):
                rows_as_lists.append(list(row))
            for j in range(0, len(rows_as_lists)):
                t = "{0:<10}{1:<8.2f}{2:<8.2f}{3:<}\n".format(rows_as_lists[j][0], rows_as_lists[j][1],
                                                              rows_as_lists[j][2], rows_as_lists[j][3])
                s = s + t
            send_email_and_record(s, 4, '异常4')
    except Exception as e:
        print("func4_send_email{}", e)


def function4(today_date, monitor_hour=""):
    """
    :param today_date:当前日期（振荡当天的，启用时间是2024-06-06 00：00，那么日期应该是2024-06-05）
    :param monitor_hour:该功能启用的时间(字符串)
    :return: 返回发送邮件的内容
    """
    name_max_min_open_closing_amplitude_percentage = pd.read_csv(f"../amplitude/{today_date.strftime("%Y-%m-%d")}.csv")

    # 提取出所有股票的name、closing_price
    today_stock = name_max_min_open_closing_amplitude_percentage.iloc[:, [0, 4]].copy()
    # 找出今天振荡幅度大于等于16%的股票
    name_max_min_open_closing_amplitude_percentage = name_max_min_open_closing_amplitude_percentage[
        name_max_min_open_closing_amplitude_percentage['amplitude_percentage'] >= 16]
    # 找出今天跌的股票
    name_max_min_open_closing_amplitude_percentage = name_max_min_open_closing_amplitude_percentage[
        name_max_min_open_closing_amplitude_percentage['open_price'] * 0.98 >
        name_max_min_open_closing_amplitude_percentage['closing_price']]
    name_max_min_open_closing_amplitude_percentage = name_max_min_open_closing_amplitude_percentage[
        name_max_min_open_closing_amplitude_percentage['min_price'] <
        name_max_min_open_closing_amplitude_percentage['open_price'] * 0.92
        ]
    name_max_min_open_closing_amplitude_percentage = name_max_min_open_closing_amplitude_percentage.drop(
        columns=['max_price', 'min_price', 'open_price', 'amplitude_percentage']
    )
    # 还剩name、closing_price 2列
    today_date_str = today_date.strftime("%Y-%m-%d")
    name_max_min_open_closing_amplitude_percentage.to_csv(
        "../function4_monitor_stock/" + today_date_str + monitor_hour + ".csv", index=False)

    df = pd.DataFrame()
    for i in range(1, 7):
        monitor_day = today_date - timedelta(days=i)
        try:
            amplitude_over21_df = pd.read_csv(
                "../function4_monitor_stock/" + monitor_day.strftime("%Y-%m-%d") + monitor_hour + ".csv")
        except:
            continue
        num = amplitude_over21_df.shape[0]
        if num == 0:
            continue
        else:
            match_df = pd.merge(today_stock, amplitude_over21_df, on='name', how='right')
            match_df = match_df[match_df.iloc[:, 1] <= match_df.iloc[:, 2] * 0.92]
            num = match_df.shape[0]
            if num > 0:
                match_df['monitor_day_str'] = monitor_day.strftime("%Y-%m-%d")
                df = pd.concat([df, match_df], ignore_index=True)
    df.to_csv("../send_email_data/func4.csv", index=False)
    # 发送邮件
    func4_send_email()
