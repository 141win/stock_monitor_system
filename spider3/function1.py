# _*_ coding: UTF-8 _*_
# @Time : 2024/6/27 12:13
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : function1.py
# @Project : stockSpiderAndstockMonitor
from datetime import timedelta
import pandas as pd
from record_sendemail_state import send_email_and_record
# import objgraph
# import gc

"""
使用到的目录有
send_email_data
one_hour_rise_fall
one_hour_rise_4.5
"""


def func1_send_email():
    # show_memory_info('after a created')
    try:
        df = pd.read_csv('../func1.csv')
        # 读取黑名单
        with open('../black_list.txt', 'r') as f:
            lines = f.readlines()
            black_list = [i.strip() for i in lines]
        # 根据黑名单去掉黑名单中的股票
        df = df[~df['name'].isin(black_list)]
        if df.shape[0] > 0:
            s = str(df.shape[0]) + '支股票异常：某一小时内涨幅超过4.5%并且9小时内比涨之前跌超3.5%\n'
            s = s + "{0:<10}{1:<15}{2:<}\n".format('名称', '观测时间', '现价时间')
            # 将df按照名字排序
            df = df.sort_values(by=['name'])
            # 发送邮件
            # 此时还有name、异常时间、当前时间
            rows_as_lists = []
            for index, row in df.iterrows():
                rows_as_lists.append(list(row))
            for j in range(0, len(rows_as_lists)):
                # 将每一行转换为字符串时，保证每一列对齐
                t = "{0:<10}{1:<15}{2:<}\n".format(rows_as_lists[j][0], rows_as_lists[j][1], rows_as_lists[j][2])
                s = s + t
            send_email_and_record(s, 1, '异常1')
            # show_memory_info('after a created')
            # del lines, f, black_list
            # del s
            # del df
            # del rows_as_lists
            # gc.collect()
            # del send_email_and_record
            # objgraph.show_refs([s])
            # objgraph.show_refs([df])
            # objgraph.show_refs([rows_as_lists])
    except Exception as e:
        print("func1_send_email{}", e)


def function1(now_time):
    """
    此模块是第一个功能的控制模块，
    :param now_time:
    """
    # 读取涨跌幅和价格
    rise_fall_percentage = pd.read_csv(f'../one_hour_rise_fall/{now_time.strftime("%Y-%m-%d %H")}.csv')

    now_hour_price = rise_fall_percentage.iloc[:, [0, 2]].copy()
    # 选取涨幅大于等于4.5%的股票
    rise_fall_percentage = rise_fall_percentage[rise_fall_percentage['rise_fall_percentage'] >= 4.5]
    # 删除第3、第4列，保留name、以及涨前价
    rise_fall_percentage = rise_fall_percentage.drop(columns=rise_fall_percentage.columns[2:])
    # 将涨跌幅大于等于4.5%的股票保存到对应小时的csv文件
    rise_fall_percentage.to_csv(f'../one_hour_rise_4.5/{now_time.strftime("%Y-%m-%d %H")}.csv', index=False)

    df = pd.DataFrame()
    for i in range(1, 10):
        monitor_time = now_time - timedelta(hours=i)
        try:
            one_hour_rise_045_stock = pd.read_csv(
                f'../one_hour_rise_4.5/{monitor_time.strftime("%Y-%m-%d %H")}.csv')
        except:
            continue
        if one_hour_rise_045_stock.shape[0] == 0:
            continue
        else:
            # 将one_hour_rise_045_stock与now_hour_price按name拼接
            one_hour_rise_045_stock = pd.merge(one_hour_rise_045_stock, now_hour_price, on='name', how='left')
            nine_hour_fall_035_stock = one_hour_rise_045_stock[one_hour_rise_045_stock.iloc[:, 2] <=
                                                               one_hour_rise_045_stock.iloc[:, 1] * 0.965]
            if nine_hour_fall_035_stock.shape[0] > 0:
                nine_hour_fall_035_stock = nine_hour_fall_035_stock.drop(columns=nine_hour_fall_035_stock.columns[1:])
                nine_hour_fall_035_stock['monitor_hour'] = monitor_time.strftime("%Y-%m-%d %H")
                nine_hour_fall_035_stock['now_hour'] = now_time.strftime("%Y-%m-%d %H")
                df = pd.concat([df, nine_hour_fall_035_stock], ignore_index=True)

    df.to_csv("../send_email_data/func1.csv", index=False)
    # 发送邮件
    func1_send_email()

#
# import os
# import psutil
#
#
# def show_memory_info(hint):
#     pid = os.getpid()
#     p = psutil.Process(pid)
#
#     info = p.memory_full_info()
#     memory = info.uss / 1024. / 1024
#     print('{} memory used: {} MB'.format(hint, memory))
#     del pid, p, info, memory
#     gc.collect()


# def func():
#     show_memory_info('initial')
#     func1_send_email()
#     gc.collect()
#     show_memory_info('after a created')
#
#
# func()
# show_memory_info('finished')
# gc.collect()
# # a = [1, 2, 3]
# # b = [4, 5, 6]
# #
# # a.append(b)
# # b.append(a)
# #
# # objgraph.show_refs([a])
# # objgraph.show_backrefs([a])
