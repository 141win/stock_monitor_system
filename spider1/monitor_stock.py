# _*_ coding: UTF-8 _*_
# @Time : 2024/5/10 17:27
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : monitor_stock.py
# @Project : stockSpiderAndstockMonitor

import numpy as np
import send_email
import pandas as pd
from datetime import datetime, timedelta


# 算涨幅
def calculate_rise_pct(price_table):
    pct_ch = []
    for i in range(1, len(price_table)):
        pct = (price_table[i] - price_table[i - 1]) / price_table[i - 1]
        pct_ch.append(pct)
    return pct_ch


# 提取价格
# refer_time:多少个小时
def extract_price(refer_time, csv_path):
    df1 = pd.read_csv(csv_path)
    # k表示需要多少个价格
    k = refer_time + 1
    df1 = (df1.T.tail(k)[0])
    # price_table存提取出来的价格
    price_table = df1.tolist()
    # 返回价格
    return price_table


# refer_time:参考时间，例如以三个小时前的价格为参考就输入三个小时
# 寻找一小时内涨幅超过4.5%的股票
def one_hour_rise_045(names, refer_time, now_hour):
    # 记录符合条件的股票数量
    s1_num = 0
    # 记录符合条件的股票名称
    s1_name = []
    # 记录符合条件的股票前一个小时价格
    s1_price = []
    for nm in names:
        # 有可能表中只有一个数据，提取数据时会出错
        try:
            price_table = extract_price(refer_time,
                                        '../every_stock_excel/' + str(nm) + '.csv')
            price_pct = calculate_rise_pct(price_table)
            if price_pct[0] >= 0.045:
                # 存储该股票名称以及前一个小时的价格
                s1_num += 1
                s1_price.append(price_table[0])
                s1_name.append(nm)
        except Exception as e:
            print(nm, e)

    # 将符合条件的股票数据存入当前小时对应的excel表中
    # 将股票数据存入DataFrame中
    rdf = pd.DataFrame({'name': s1_name, 'price': s1_price})
    # 存入excel中
    rdf.to_csv('../one_hour_rise/' + str(now_hour) + '.csv', index=False)


# 监测近9小时内某小时涨幅超过4.5%股票的现价是否低于涨前的3.5%
def nine_hour_fall_035(now_hour):
    # 记录符合条件的股票数量
    s1_num = 0
    # 记录符合条件的股票信息
    s1 = []
    for i in range(1, 10):
        # 开始监测的时间
        monitor_hour = now_hour - i
        # 读取监测股票信息
        if monitor_hour < 0:
            monitor_hour = 24 + monitor_hour
        try:
            df = pd.read_csv(
                '../one_hour_rise/' + str(monitor_hour) + '.csv')
        except:
            continue
        if df.shape[0] == 0:
            continue
        else:
            name_list = df['name'].tolist()
            price_list = df['price'].tolist()
            for j in range(0, len(name_list)):
                try:
                    price = extract_price(0, '../every_stock_excel/' + name_list[j] + '.csv')
                    price_table = [float(price_list[j]), price[0]]
                    # 如果股票现价比观测价格低百分之3.5就记录发邮件
                    if calculate_rise_pct(price_table)[0] <= -0.035:
                        s1_num += 1
                        s1.append(
                            [name_list[j], str(price_table[0]), str(price_table[1]), str(monitor_hour), str(now_hour)])
                except Exception as e:
                    print(e)
    if s1_num > 0:
        text = '名称 观测价格 现价 观测时间 现价时间'
        s = ''
        for i in range(0, s1_num):
            t = '   '.join(s1[i])
            s = s + t + "\n"
        text = text + "\n" + s
        # 发邮件
        send_email.send(str(s1_num) + '支股票异常,某一小时内涨幅超过4.5%并且9小时内比涨之前跌超3.5%' + "\n" + text,
                        "13086397065@163.com", "3145971793@qq.com", "2285687467@qq.com")


# 寻找近三十天最高收盘价并且寻找今天跌且比近三十天最高收盘价跌45%的股票
def one_month_max_closing_price_and_search_target_stock(closing_price_path, max_closing_price_path):
    # 有可能表中数据不够就会报错
    try:
        # 读取收盘价
        df_closing_price = pd.read_csv(closing_price_path)
        df_no_na = df_closing_price.dropna(axis=0)
        # 从每行的最后30列找出最大值
        # 在Python中，索引是从0开始的，所以这里我们用iloc[:, -30:]来选择从倒数30列到最后一列
        if df_no_na.shape[1] < 31:
            max_values = df_no_na.iloc[:, -(df_no_na.shape[1] - 1):].max(axis=1)
        else:
            max_values = df_no_na.iloc[:, -30:].max(axis=1)
        # max_values现在是一个Series，包含了每行最后30列的最大值
        # 将其转换为DataFrame
        df_max_closing_price = pd.DataFrame({'name': df_no_na['name'], 'max_closing_price': max_values})
        # 存入近30天最大收盘价表中
        df_max_closing_price.to_csv(max_closing_price_path, index=False)

        # 寻找今天跌且比近三十天最高收盘价跌55%的股票

        if df_no_na.shape[1] <= 2:
            comparison = (df_no_na.iloc[:, -1] == df_no_na.iloc[:, -1]).astype(int) * 2 - 1
        else:
            comparison = (df_no_na.iloc[:, -1] >= df_no_na.iloc[:, -2]).astype(int) * 2 - 1

        # 将比较结果与第一列name列对应起来
        # 使用DataFrame的构造函数来创建一个新的DataFrame
        result_df = pd.DataFrame({
            'name': df_no_na['name'],
            'comparison': comparison
        })
        # 按name列拼接
        # 提取df_no_na name列和最后一列
        df_latest_closing_price = df_no_na[['name', df_no_na.columns[-1]]].rename(
            columns={df_no_na.columns[-1]: 'latest_closing_price'})  # 重命名列以避免冲突

        # 将result_df、df1_last_price和df2_max_price按name列合并
        merged_df = pd.merge(result_df, df_latest_closing_price, on='name', how='left')
        merged_df = pd.merge(merged_df, df_max_closing_price, on='name', how='left')
        df_no_no_merged = merged_df.dropna(axis=0)
        # # 尝试将价格列转换为浮点数，使用 errors='coerce' 将无法转换的值设置为 NaN
        # merged_df['latest_closing_price'] = pd.to_numeric(merged_df['latest_closing_price'], errors='coerce')
        # merged_df['max_closing_price'] = pd.to_numeric(merged_df['max_closing_price'], errors='coerce')

        # 计算下跌百分比（仅当comparison为-1时）
        df_no_no_merged['price_drop_percentage'] = np.where(df_no_no_merged['comparison'] == -1, (
                (df_no_no_merged['latest_closing_price'] - df_no_no_merged['max_closing_price']) / df_no_no_merged[
            'max_closing_price']), np.nan)  # 使用np.nan填充非下跌行

        # 找出下跌超过55%的
        dropped_over_55_df = df_no_no_merged[df_no_no_merged['price_drop_percentage'] <= -0.45]
        if dropped_over_55_df.shape[0] > 0:
            # 删除comparison列
            df = dropped_over_55_df.drop(columns=['comparison'])
            rows_as_lists = []
            for index, row in df.iterrows():
                rows_as_lists.append(list(row))
            text = '名称 收盘价 最高收盘价 跌幅'
            s = ''
            for i in range(0, dropped_over_55_df.shape[0]):
                rows_as_lists[i][1] = str(rows_as_lists[i][1])
                rows_as_lists[i][2] = str(rows_as_lists[i][2])
                rows_as_lists[i][3] = str(rows_as_lists[i][3])
                t = '   '.join(rows_as_lists[i])
                s = s + t + "\n"
            # 发邮件
            send_email.send(
                str(dropped_over_55_df.shape[
                        0]) + '支股票异常,今天下跌，且今天收盘价比近三十天最高收盘价跌超45%' + "\n" + text + "\n" + s,
                "13086397065@163.com", "3145971793@qq.com", "2285687467@qq.com")
    except Exception as e:
        print(e)


# 寻找今天跌且振荡幅度超过21%的股票并记录 ，以及寻找今天跌且七天内振荡，而且股价为开始的95%
def search_today_fall_amplitude_over21_and_today_fall_seven_day_amplitude_95(df, today, closing_price_path, csv_path):
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    # 先找出今天跌的股票
    today_fall_df = search_today_fall(closing_price_path, yesterday)
    # 五分钟一次，一天需要找出288个记录值
    # 读今天跌的股票288个记录值
    # 提取今天跌的股票名称
    names = today_fall_df['name'].tolist()
    # 找出今天跌的股票的数据
    matched_rows = df[df['name'].isin(names)]
    if len(df.columns) > 289:
        selected_columns = matched_rows.iloc[:, :1].join(matched_rows.iloc[:, -288:])
        # 找出最大值最小值
        max_values = selected_columns.iloc[:, -288:].max(axis=1)
        min_values = selected_columns.iloc[:, -288:].min(axis=1)
        open_price = selected_columns.iloc[:, 1].tolist()
        max_min_values_df = pd.DataFrame(
            {'name': selected_columns['name'], 'max_values': max_values, 'min_values': min_values,
             'open_price': open_price}, )
    else:
        # 如果列数不够，则选择所有列
        selected_columns = matched_rows
        max_values = selected_columns.iloc[:, -(selected_columns.shape[1] - 1):].max(axis=1)
        min_values = selected_columns.iloc[:, -(selected_columns.shape[1] - 1):].min(axis=1)
        open_price = selected_columns.iloc[:, 1].tolist()
        max_min_values_df = pd.DataFrame(
            {'name': selected_columns['name'], 'max_values': max_values, 'min_values': min_values,
             'open_price': open_price}, )
    max_min_values_df['amplitude_percentage'] = (
            (max_min_values_df['max_values'] - max_min_values_df['min_values']) / max_min_values_df['open_price'])
    amplitude_over21_df = max_min_values_df[max_min_values_df['amplitude_percentage'] >= 0.21]
    # 获取星期几的整数表示（0代表星期一，1代表星期二，...，6代表星期日）
    # 获取振荡当天的星期几的整数表示
    now = (today - timedelta(days=1)).weekday()
    amplitude_over21_df.to_csv('../amplitude_over21/' + str(now) + csv_path, index=False)

    # 寻找今天跌且七天内振荡，而且股价为开始的95%
    s1_num = 0
    s1 = ''
    for i in range(1, 7):
        monitor_weekday = now - i
        if monitor_weekday < 0:
            monitor_weekday = 7 + monitor_weekday
        try:
            # 读取振幅超过21%的股票数据
            amplitude_over21_df = pd.read_csv(
                '../amplitude_over21/' + str(monitor_weekday) + csv_path)
        except Exception as e:
            print(e)
            continue
        # 找出今天跌的振荡超过21%的股票数据
        matched_stock = pd.merge(amplitude_over21_df, today_fall_df, on='name', how='left')
        matched_stock.dropna(axis=0)
        if matched_stock.shape[0] == 0:
            continue
        matched_stock['fall_pct'] = (matched_stock['closing_price'] / matched_stock['open_price'])
        matched_stock = matched_stock[matched_stock['fall_pct'] <= 0.95]
        # 此时matched_stock有'name'，'max_values'，'min_values'，'open_price'，'amplitude_percentage'，'comparison'，'fall_pct','closing_price'
        if matched_stock.shape[0] > 0:
            df1 = matched_stock.drop(columns=['max_values', 'min_values', 'fall_pct', 'comparison'])
            # 还有name\open_price\amplitude_percentage\closing_price
            rows_as_lists = []
            for index, row in df1.iterrows():
                rows_as_lists.append(list(row))
            # 计算出振荡当天的日期
            amplitude_date = str((today - timedelta(days=i + 1)).strftime("%Y-%m-%d"))
            for j in range(0, df1.shape[0]):
                s1_num += 1
                rows_as_lists[j][1] = str(rows_as_lists[j][1])
                rows_as_lists[j][2] = str(rows_as_lists[j][2])
                rows_as_lists[j][3] = str(rows_as_lists[j][3])
                t = '   '.join(rows_as_lists[j])
                s1 = s1 + t + "   " + amplitude_date + "\n"
    if s1_num > 0:
        text = '名称 起始价 振荡幅度 收盘价 振荡日期'
        text = text + "\n" + s1
        # 发邮件
        send_email.send(
            str(s1_num) + '支股票异常,最近7天中某天股价下跌且振荡幅度超过21%，今天也下跌，收盘价小于等于振荡当天的开盘价的95%' + "\n" + text,
            "13086397065@163.com", "3145971793@qq.com", "2285687467@qq.com")


# 寻找今天跌的股票（收盘和开盘比）
def search_today_fall(csv_path, yesterday):
    # 读取收盘价
    df1 = pd.read_csv(csv_path)
    # 计算最后两列价格的比较结果
    # 注意这里我们使用iloc[-2]和iloc[-1]来获取最后两列，因为它们是基于位置的索引
    # 当今天的收盘价比开盘价（昨天的收盘价）低，就为-1，其余为1
    df_no_na = df1.dropna(axis=0)
    if df_no_na.shape[1] <= 2:
        comparison = (df_no_na.iloc[:, -1] == df_no_na.iloc[:, -1]).astype(int) * 2 - 1
    else:
        comparison = (df_no_na.iloc[:, -1] >= df_no_na.iloc[:, -2]).astype(int) * 2 - 1
    # 将比较结果与前一列'name'对应起来
    # 使用DataFrame的构造函数来创建一个新的DataFrame
    result_df = pd.DataFrame({
        'name': df_no_na['name'],
        'comparison': comparison,
        'closing_price': df_no_na[yesterday]
    })
    # 找出今天跌的股票并返回
    # bool索引查找
    mask = result_df['comparison'] == -1
    return result_df[mask]
