# _*_ coding: UTF-8 _*_
# @Time : 2024/7/2 11:12
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : function5.py
# @Project : stockSpiderAndstockMonitor
import calculate_module as cal
import pandas as pd
from datetime import timedelta
from record_sendemail_state import send_email_and_record

"""
使用到的目录有
send_email_data
one_hour_amplitude
function5_monitor_stock
all_stock_hour_price
"""


def func5_send_email():
    try:
        df = pd.read_csv('../send_email_data/func5.csv')
        if df.shape[0] > 0:
            s = str(df.shape[0]) + '支股票异常：某一小时涨超1%，涨前24小时内至少有一次一小时振幅超过3%，涨后9小时内跌倒涨前的98%之下\n'
            s = s + "{0:<10}{1:<5}{2:<}\n".format('名称', '涨的时间', '跌的时间')
            # 将df按照名字排序
            df = df.sort_values(by=['name'])
            # 发送邮件
            # 此时还有name、异常时间、当前时间
            rows_as_lists = []
            for index, row in df.iterrows():
                rows_as_lists.append(list(row))
            for j in range(0, len(rows_as_lists)):
                # 将每一行转换为字符串时，保证每一列对齐
                t = "{0:<10}{1:<15}{2:<10}\n".format(rows_as_lists[j][0], rows_as_lists[j][1], rows_as_lists[j][2])
                s = s + t
            send_email_and_record(s, 5, '异常5')
    except Exception as e:
        print("func5_send_email{}", e)


def func5_part1(rise_1, now_time):
    """
    :param rise_1:只有name、涨前价2列
    :param now_time:
    :return:
    """
    try:
        max_and_amplitude = pd.read_csv(
            f'../one_hour_amplitude/{(now_time - timedelta(hours=1)).strftime("%Y-%m-%d %H")}.csv')
        all_max_price = max_and_amplitude.iloc[:, [0, 1]].copy()
        all_max_price = all_max_price.rename(columns={'max_price': "1"})
        all_amplitude = max_and_amplitude.iloc[:, [0, 2]].copy()
        for i in range(2, 25):
            time = now_time - timedelta(hours=i)
            try:
                max_and_amplitude = pd.read_csv(f'../one_hour_amplitude/{time.strftime("%Y-%m-%d %H")}.csv')
                max_price = max_and_amplitude.iloc[:, [0, 1]].copy()
                max_price = max_price.rename(columns={'max_price': str(i)})
                all_max_price = pd.merge(all_max_price, max_price, on='name', how='left')

                amplitude = max_and_amplitude.iloc[:, [0, 2]].copy()
                amplitude = amplitude.rename(columns={'amplitude_percentage': str(i)})
                all_amplitude = pd.merge(all_amplitude, amplitude, on='name', how='left')

            except:
                return
        all_max_price['max_price'] = all_max_price.iloc[:, 1:].max(axis=1)
        all_max_price = all_max_price.drop(columns=all_max_price.columns[1:-1])
        # 只要24次振荡幅度中有任意一次超过3%的，则选出来
        all_amplitude["max_amplitude"] = all_amplitude.iloc[:, 1:].max(axis=1)
        # all_amplitude["max_amplitude"] = all_amplitude[all_amplitude.iloc[:, 1:].max(axis=1) >= 3].copy()
        # 提取出24小时内至少有一次振荡幅度超过3%的股票名称
        name = all_amplitude[all_amplitude['max_amplitude'] >= 3].iloc[:, 0].copy()

        merged = pd.merge(all_max_price, name, on='name', how='inner')

        merged = pd.merge(merged, rise_1, on='name', how='inner')
        # name、max_price、涨前价
        merged = merged[merged['max_price'] * 0.93 >= merged.iloc[:, -1]]
        merged.to_csv(f"../function5_monitor_stock/{now_time.strftime('%Y-%m-%d %H')}.csv", index=False)
    except Exception as e:
        print(e)


def func5_part2(rise_1, now_time):
    """

    :param rise_1: 只有name、当前价（跌后价）
    :param now_time:
    :return:
    """
    df = pd.DataFrame()
    for i in range(1, 10):
        time = now_time - timedelta(hours=i)
        # 读取监测股票信息
        try:
            monitor = pd.read_csv(
                f'../function5_monitor_stock/{time.strftime("%Y-%m-%d %H")}.csv')
        except:
            continue
        else:

            monitor = pd.merge(monitor, rise_1, on='name', how='inner')
            monitor = monitor[monitor.iloc[:, 2] * 0.98 >=
                              monitor.iloc[:, 3]]
            if monitor.shape[0] > 0:
                monitor = monitor.drop(columns=monitor.columns[1:])
                monitor['monitor_hour'] = str(time.strftime('%Y-%m-%d %H'))
                monitor['now_hour'] = str(now_time.strftime('%Y-%m-%d %H'))
                df = pd.concat([df, monitor], ignore_index=True)
    df.to_csv('../send_email_data/func5.csv', index=False)


def function5(now_time):
    # 读取每小时涨跌幅以及当前价和一小时前的价格
    rise_fall_percentage = pd.read_csv(f'../one_hour_rise_fall/{now_time.strftime('%Y-%m-%d %H')}.csv')
    # 选取涨幅大于等于1%的股票的涨前价
    now_price_rise = rise_fall_percentage[rise_fall_percentage['rise_fall_percentage'] >= 1].iloc[:, [0, 1]].copy()
    # 选取当前价格
    now_price = rise_fall_percentage.iloc[:, [0, 2]].copy()

    # 读取当前一小时的所有价格
    one_hour_all_price = pd.read_csv(f'../all_stock_hour_price/{now_time.strftime('%Y-%m-%d %H')}.csv')
    # 计算1小时的振荡幅度
    one_hour_amplitude = cal.calculate_amplitude(one_hour_all_price)

    # 将name、最大值、amplitude记录
    one_hour_amplitude = one_hour_amplitude.iloc[:, [0, 1, 5]].copy()
    one_hour_amplitude.to_csv(f"../one_hour_amplitude/{now_time.strftime('%Y-%m-%d %H')}.csv", index=False)

    func5_part1(now_price_rise, now_time)
    func5_part2(now_price, now_time)
    func5_send_email()
