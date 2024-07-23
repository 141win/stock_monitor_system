# _*_ coding: UTF-8 _*_
# @Time : 2024/6/4 20:58
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : monitor_function.py
# @Project : stockSpiderAndstockMonitor

import csv
import os
import pandas as pd
import time
from datetime import timedelta, datetime


# 统计今天发送邮件的情况
def count_today_send_email(function_list, today):
    """
    :param function_list:传入功能字典
    :param today:传入统计的日期(str)
    :return:
    """
    # 读取今天发邮件的情况的记录
    try:
        today_send_email_df = pd.read_csv('../every_day_send_email_record/' + today + '.csv')
        if today_send_email_df.shape[0] > 0:
            # 获取今天发送邮件的次数
            # 导出today_send_email_num的function_id列（1代表功能一，2代表功能2，3代表功能3。）
            function_id = today_send_email_df['function_id'].tolist()
            function_id_count_dict = {}
            for i in function_list:
                function_id_count_dict[i] = 0
            # 以字典的形式存储每一个功能发送邮件的次数，键是功能编号，值是发送邮件的次数（默认值是0），读取到某个功能的编号该功能发送邮件的次数加1
            for i in function_id:
                function_id_count_dict[i] = function_id_count_dict[i] + 1
            # 返回以字典形式存储的每个功能放松邮件的次数
            text = "今日每个发送邮件的次数\n"
            # 将function_id_count_dict和function_list进行合并，将function_id_count_dict和function_list的相同键的值合并成一个字典，键是function_list的值，值是function_id_count_dict的值合并
            function = {function_list[i]: function_id_count_dict[i] for i in function_list}
            for i in function:
                text = text + i + "：" + str(function[i]) + "次\n"
            return text
        else:
            return None
    except:
        return None


# 增加发送邮件的情况的记录
def record_today_send_email_state(text, function_id):
    """

    :param text:发送邮件的内容
    :param function_id: 发送邮件的功能id
    :return:
    """
    # 如果邮件内容不为空，就将发送邮件的功能id以及发送时间写入csv文件中。
    if text is not None:
        # 获取当前时间
        now_date = datetime.now()
        # 将当前时间转换为字符串
        now_str = now_date.strftime("%Y-%m-%d %H:%M:%S")
        # df = df.append({'function_id': function_id, 'send_time': now_str}, ignore_index=True)
        now_hour = int(time.strftime("%H"))
        data = [function_id, now_str]
        # 凌晨一点存在另一个表中
        if now_hour == 0:
            csv_path = "../every_day_send_email_record/" + (now_date - timedelta(days=1)).strftime("%Y-%m-%d") + ".csv"
        else:
            csv_path = "../every_day_send_email_record/" + now_date.strftime("%Y-%m-%d") + ".csv"
        if not os.path.exists(csv_path):
            df = pd.DataFrame([data], columns=['function_id', 'send_time'])
            df.to_csv(csv_path, index=False)
        else:
            with open(csv_path, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(data)
                print("写入成功")


# 计算涨跌幅模块
def calculate_rise_fall_percentage(name_price1_price2):
    """
    传入m行3列的dataframe数据——第一列是name、第二列是price1、第三列是price2，计算涨跌幅
    :param name_price1_price2:
    :return: 返回m行4列的dataframe数据——第一列是name、第二列是price1、第三列是price2，第四列是涨跌幅
    """
    name_price1_price2['rise_fall_percentage'] = ((name_price1_price2.iloc[:, 2] -
                                                   name_price1_price2.iloc[:, 1]) /
                                                  name_price1_price2.iloc[:, 1] * 100)

    return name_price1_price2


# 计算振荡幅度
def calculate_amplitude(one_day_price):
    """
    传入m行290列的数据（可能少于290行），第一列是name，后面所有列是每五分钟的价格，计算振幅
    :param one_day_price:
    :return:one_day:有name、max_price、min_price、open_price、amplitude_percentage
    """
    one_day_price['max_price'] = one_day_price.iloc[:, 1:].max(axis=1)
    one_day_price['min_price'] = one_day_price.iloc[:, 1:-1].min(axis=1)
    one_day_price['open_price'] = one_day_price.iloc[:, 1]
    one_day_price['closing_price'] = one_day_price.iloc[:, -4]
    one_day = one_day_price.iloc[:, [0, -4, -3, -2, -1]].copy()
    one_day['amplitude_percentage'] = (one_day['max_price'] - one_day['min_price']) / one_day['open_price'] * 100
    return one_day


# 功能1
def function1(name_price1_price2, now_hour):
    """
    此模块是第一个功能的控制模块，从上层控制模块传入name_price1_price2,now_hour两个参数
    :param name_price1_price2:
    :param now_hour: 当前小时的小时数（int）
    :return:返回发送邮件的内容
    """
    now_hour_price = name_price1_price2.iloc[:, [0, 2]]
    # 计算涨跌幅
    rise_fall_percentage = calculate_rise_fall_percentage(name_price1_price2)
    # 选取涨幅大于等于4.5%的股票
    rise_fall_percentage = rise_fall_percentage[rise_fall_percentage['rise_fall_percentage'] >= 4.5]
    # 删除第3、第4列，保留name、以及涨前价
    rise_fall_percentage = rise_fall_percentage.drop(columns=rise_fall_percentage.columns[2:])
    # 将涨跌幅大于等于4.5%的股票保存到对应小时的csv文件
    rise_fall_percentage.to_csv('../one_hour_rise_4.5/' + str(now_hour) + '.csv', index=False)

    s = ''
    # 记录符合条件的股票数量
    s1_num = 0
    for i in range(1, 10):
        monitor_hour = now_hour - i
        # 读取监测股票信息
        if monitor_hour < 0:
            monitor_hour = 24 + monitor_hour
        try:
            one_hour_rise_045_stock = pd.read_csv(
                '../one_hour_rise_4.5/' + str(monitor_hour) + '.csv')
        except:
            continue
        if one_hour_rise_045_stock.shape[0] == 0:
            continue
        else:
            # 将one_hour_rise_045_stock与now_hour_price按name拼接
            one_hour_rise_045_stock = pd.merge(one_hour_rise_045_stock, now_hour_price, on='name', how='left')
            nine_hour_fall_035_stock = one_hour_rise_045_stock[one_hour_rise_045_stock.iloc[:, 2] <
                                                               one_hour_rise_045_stock.iloc[:, 1] * 0.965]
            if nine_hour_fall_035_stock.shape[0] > 0:
                s1_num += nine_hour_fall_035_stock.shape[0]
                # 此时还有name、monitor_hour_price、now_hour_price
                rows_as_lists = []
                for index, row in nine_hour_fall_035_stock.iterrows():
                    rows_as_lists.append(list(row))
                for j in range(0, len(rows_as_lists)):
                    rows_as_lists[j][1] = str(rows_as_lists[j][1])
                    rows_as_lists[j][2] = str(rows_as_lists[j][2])
                    t = '   '.join(rows_as_lists[j])
                    s = s + t + '   ' + str(monitor_hour) + '   ' + str(now_hour) + "\n"
    if s1_num > 0:
        return str(
            s1_num) + '支股票异常：某一小时内涨幅超过4.5%并且9小时内比涨之前跌超3.5%' + "\n" + '名称 观测价格 现价 观测时间 现价时间' + "\n" + s


# 找出最大值
def search_max_closing_price(thirty_day_closing_price):
    """
    :param thirty_day_closing_price:
    :return:one_day:有name、max_price、min_price、open_price、amplitude_percentage
    """
    thirty_day_closing_price['max_price'] = thirty_day_closing_price.iloc[:, 1:].max(axis=1)
    thirty_day_closing_price = thirty_day_closing_price.iloc[:, [0, -1]]
    return thirty_day_closing_price


# 功能2
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
    merged_df = calculate_rise_fall_percentage(merged_df)
    merged_df = merged_df[merged_df['rise_fall_percentage'] <= -45]

    s = ''
    # 记录符合条件的股票数量
    s1_num = merged_df.shape[0]
    if s1_num > 0:
        rows_as_lists = []
        for index, row in merged_df.iterrows():
            rows_as_lists.append(list(row))
        for i in range(0, s1_num):
            rows_as_lists[i][1] = str(rows_as_lists[i][1])
            rows_as_lists[i][2] = str(rows_as_lists[i][2])
            rows_as_lists[i][3] = str(rows_as_lists[i][3])
            t = '   '.join(rows_as_lists[i])
            s = s + t + "\n"
        # 发邮件
        return str(
            s1_num) + '支股票异常：今天下跌，且今天收盘价比近三十天最高收盘价跌超45%' + "\n" + '名称 最高收盘价 今日收盘价 跌幅' + "\n" + s


# 功能3
def function3(name_max_min_open_closing_amplitude_percentage, today_date, monitor_hour=""):
    """
    :param name_max_min_open_closing_amplitude_percentage:今天的最高价、最低价、开盘价、收盘价、振幅
    :param today_date:当前日期（振荡当天的，启用时间是2024-06-06 00：00，那么日期应该是2024-06-05）
    :param monitor_hour:该功能启用的时间(字符串)
    :return: 返回发送邮件的内容
    """
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
    name_max_min_open_closing_amplitude_percentage.to_csv(
        "../fall_and_amplitude_over21/" + today_date_str + monitor_hour + ".csv", index=False)

    # 记录符合条件的股票数
    s1_num = 0
    # 记录邮件内容
    s1 = ''
    for i in range(1, 7):
        monitor_day = today_date - timedelta(days=i)
        monitor_day_str = monitor_day.strftime("%Y-%m-%d")
        try:
            amplitude_over21_df = pd.read_csv("../fall_and_amplitude_over21/" + monitor_day_str + monitor_hour + ".csv")
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
                s1_num += num
                # 还剩name、今日closing_price、振荡closing_price 3列
                rows_as_lists = []
                for index, row in (match_df.iterrows()):
                    rows_as_lists.append(list(row))
                # 计算出振荡当天的日期
                for j in range(0, num):
                    rows_as_lists[j][1] = str(rows_as_lists[j][1])
                    rows_as_lists[j][2] = str(rows_as_lists[j][2])
                    t = '   '.join(rows_as_lists[j])
                    s1 = s1 + t + "   " + monitor_day_str + "\n"
    if s1_num > 0:
        # 发邮件
        return str(
            s1_num) + '支股票异常：最近7天中某天股价下跌且振荡幅度超过21%，今天也下跌，收盘价小于等于振荡当天的收盘价的95%' + "\n" + '名称 今日收盘价 振荡收盘价 振荡日期' + "\n" + s1


# 功能4
def function4(name_max_min_open_closing_amplitude_percentage, today_date, monitor_hour=""):
    """
    :param name_max_min_open_closing_amplitude_percentage:今天的最高价、最低价、开盘价、收盘价、振幅
    :param today_date:当前日期（振荡当天的，启用时间是2024-06-06 00：00，那么日期应该是2024-06-05）
    :param monitor_hour:该功能启用的时间(字符串)
    :return: 返回发送邮件的内容
    """
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

    # 记录符合条件的股票数
    s1_num = 0
    # 记录邮件内容
    s1 = ''
    for i in range(1, 7):
        monitor_day = today_date - timedelta(days=i)
        monitor_day_str = monitor_day.strftime("%Y-%m-%d")
        try:
            amplitude_over21_df = pd.read_csv("../fall_and_amplitude_over21/" + monitor_day_str + monitor_hour + ".csv")
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
                s1_num += num
                # 还剩name、今日closing_price、振荡closing_price 3列
                rows_as_lists = []
                for index, row in (match_df.iterrows()):
                    rows_as_lists.append(list(row))
                # 计算出振荡当天的日期
                for j in range(0, num):
                    rows_as_lists[j][1] = str(rows_as_lists[j][1])
                    rows_as_lists[j][2] = str(rows_as_lists[j][2])
                    t = '   '.join(rows_as_lists[j])
                    s1 = s1 + t + "   " + monitor_day_str + "\n"
    if s1_num > 0:
        # 发邮件
        return (str(
            s1_num) + '支股票异常：最近7天中某天股价跌幅超过2%、振荡幅度超过16%、最低价小于等于开盘价的92%；今日收盘价小于等于振荡当天的收盘价的92%' + "\n"
                + '名称 今日收盘价 振荡收盘价 振荡日期' + "\n" + s1)
