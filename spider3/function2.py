# _*_ coding: UTF-8 _*_
# @Time : 2024/6/27 12:16
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : function2.py
# @Project : stockSpiderAndstockMonitor
import calculate_module as cal
import pandas as pd
from record_sendemail_state import send_email_and_record

"""
使用到的目录有
send_email_data
"""


def func2_send_email():
    try:
        df = pd.read_csv("../send_email_data/func2.csv")
        if df.shape[0] > 0:
            s = str(df.shape[0]) + '支股票异常：今天下跌，且今天收盘价比近三十天最高收盘价跌超45%\n'
            s = s + "{0:<10}{1:<8}{2:<8}{3:<3}\n".format("名称", "最高收盘价", "今日收盘价", "跌幅")
            rows_as_lists = []
            for index, row in df.iterrows():
                rows_as_lists.append(list(row))
            for i in range(0, df.shape[0]):
                t = "{0:<10}{1:<8.2f}{2:<8.2f}{3:<3.1f}\n".format(rows_as_lists[i][0], rows_as_lists[i][1],
                                                                  rows_as_lists[i][2],
                                                                  rows_as_lists[i][3])
                s = s + t
            # 发邮件
            send_email_and_record(s, 2, "异常2")
    except Exception as e:
        print("func2_send_email{}", e)


def function2(name_price1_price2_percentage, thirty_day_max_closing_price):
    """

    :param name_price1_price2_percentage: 昨天收盘价和今天收盘价
    :param thirty_day_max_closing_price:近三十天收盘价中的最大收盘价
    # :param today_date:当前日期
    # :param monitor_hour:该功能启用的时间
    :return: 返回发送邮件的内容
    """
    # 找出跌的股票
    name_price1_price2_percentage = name_price1_price2_percentage[
        name_price1_price2_percentage['rise_fall_percentage'] < 0]
    # 删除price1、rise_fall_percentage列
    name_price1_price2_percentage = name_price1_price2_percentage.drop(
        columns=name_price1_price2_percentage.columns[[1, 3]])
    # 拼接以后只有name、max_closing_price、price2
    merged_df = pd.merge(thirty_day_max_closing_price, name_price1_price2_percentage, on='name', how='right')
    # 计算涨跌幅
    merged_df = cal.calculate_rise_fall_percentage(merged_df)
    merged_df = merged_df[merged_df['rise_fall_percentage'] <= -45]

    merged_df.to_csv("../send_email_data/func2.csv", index=False)

    func2_send_email()
