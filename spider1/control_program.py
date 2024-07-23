# _*_ coding: UTF-8 _*_
# @Time : 2024/5/10 17:28
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : control_program.py
# @Project : stockSpiderAndstockMonitor
import send_email
import spider_bijiewang
import spider_Bi123
import time
import monitor_stock
from datetime import datetime, timedelta
import read_write_csv
import pandas as pd


# 判断是否到设置的时间启动爬虫程序和监测异常股票程序，是则返回True开始爬取，不是则休眠
def next_time(span, boot_time):
    now_minute = int(time.strftime("%M"))
    while True:
        # 如果到点则返回True开始爬股票
        if now_minute % span == boot_time:
            return True
        if 4 - now_minute % 5 > 0:
            print((4 - now_minute % 5) * 60)
            time.sleep((4 - now_minute % 5) * 60)
        else:
            print(60 - int(time.strftime("%S")))
            time.sleep(60 - int(time.strftime("%S")))
        now_minute = int(time.strftime("%M"))


def process(df_stock, today, now_hour, now_minute):
    today_str = today.strftime("%Y-%m-%d %H:%M")
    # 将股票数据存入总表，并传回所有股票的所有数据
    if now_minute == 0 and now_hour == 0:
        df_all_stock = read_write_csv.write_all_stock_to_csv(df_stock,
                                                             '../all_stock/stock' + (today - timedelta(
                                                                 days=1)).strftime(
                                                                 '%Y-%m-%d') + '.csv')
        # 获取name列以及最后一列数据
        df_stock = df_all_stock.iloc[:, [0, -1]].copy()
        read_write_csv.write_all_stock_to_csv(df_stock, '../all_stock/stock' + today.strftime(
            '%Y-%m-%d') + '.csv')
    else:
        df_all_stock = read_write_csv.write_all_stock_to_csv(df_stock,
                                                             '../all_stock/stock' + today.strftime(
                                                                 '%Y-%m-%d') + '.csv')
        # 获取name列以及最后一列数据
        df_stock = df_all_stock.iloc[:, [0, -1]].copy()
    print("总表存储完成", datetime.now())
    # 如果当前分钟为0，则进行寻找和监测
    if now_minute == 0:
        try:
            # 将每支股票存入分表
            read_write_csv.write_every_stock_to_csv(df_stock, today_str)
            # 删除空值行
            # 导出股票名称
            names = df_stock.dropna(axis=0)['name'].tolist()
            # 寻找一小时涨幅超过0.45的股票
            monitor_stock.one_hour_rise_045(names, 1, now_hour)
            # 监测一小时涨幅超过0.45的股票是否九小时内跌幅超过0.35
            monitor_stock.nine_hour_fall_035(now_hour)
            print("第一个功能完成")
        except Exception as e:
            print(e)
        # 如果当前小时为0，则进行寻找和监测
        if now_hour == 0:
            # 使用timedelta来减去一天，得到昨天的日期
            yesterday = today - timedelta(days=1)
            # 将昨天的日期格式化为字符串
            yesterday_str = yesterday.strftime("%Y-%m-%d")
            df_stock.rename(columns={today_str: yesterday_str}, inplace=True)
            # 存入收盘价表中（0点收盘）
            read_write_csv.write_all_stock_to_csv(df_stock, '../closing_price/closing_price.csv')
            try:
                # 30天内最大收盘价，今日跌且收盘价小于等于30日内最大收盘价的55%
                monitor_stock.one_month_max_closing_price_and_search_target_stock(
                    '../closing_price/closing_price.csv', '../closing_price/max_closing_price.csv')
                print("第二个功能完成", datetime.now())
                # 寻找今天跌且振荡幅度超过21%的股票，寻找今日跌且近7日有振荡，收盘价小于等于振荡当天开盘价95%
                monitor_stock.search_today_fall_amplitude_over21_and_today_fall_seven_day_amplitude_95(
                    df_all_stock, today, "../closing_price/closing_price.csv", ".csv")
                print("第三个功能完成", datetime.now())
            except Exception as e:
                print(e)
        # 如果当前小时为8，则进行寻找和监测
        if now_hour == 8:
            try:
                yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
                df_stock.rename(columns={today_str: yesterday_str}, inplace=True)
                # 存入收盘价表中（8点收盘）
                read_write_csv.write_all_stock_to_csv(df_stock, '../closing_price/closing_price8.csv')
                # 30天内最大收盘价，今日跌且收盘价小于等于30日内最大收盘价的55%
                monitor_stock.one_month_max_closing_price_and_search_target_stock(
                    '../closing_price/closing_price8.csv', '../closing_price/max_closing_price8.csv')
                print("第二个功能完成", datetime.now())

                # 寻找今天跌且振荡幅度超过21%的股票，寻找今日跌且近7日有振荡，收盘价小于等于振荡当天开盘价95%

                # 读取昨天的总表stock数据
                df_all_stock_yesterday = pd.read_csv(
                    '../all_stock/stock' + (today - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv')
                # 删除df_all_stock_yesterday中的最后一列数据
                df_all_stock_yesterday.drop(df_all_stock_yesterday.columns[-1], axis=1, inplace=True)
                # 将df_all_stock_yesterday 与 df_all_stock拼接起来
                df_all_stock8 = pd.merge(df_all_stock_yesterday, df_all_stock, on='name')
                # 删除重复行
                df_all_stock8.drop_duplicates(subset=['name'], keep='first')
                monitor_stock.search_today_fall_amplitude_over21_and_today_fall_seven_day_amplitude_95(
                    df_all_stock8, today, "../closing_price/closing_price8.csv", "8.csv")
                print("第三个功能完成", datetime.now())
            except Exception as e:
                print(e)
                send_email.send(str(e), "13086397065@163.com")


# 每五分钟
def cycle_five_minutes():
    while True:
        now_minute = 0
        # 五分钟爬一次，整点爬
        if next_time(5, 0):
            # 获得今天的日期时间
            today = datetime.now()
            now_hour = int(time.strftime("%H"))
            now_minute = int(time.strftime("%M"))
            print(today)
            # 将当前时间格式化为字符串
            today_str = today.strftime("%Y-%m-%d %H:%M")
            try:
                # 如果第一个网站爬取失败，爬取第二个网站
                try:
                    df_stock = spider_Bi123.main(today)
                    print("爬取完成", datetime.now())
                except Exception as e:
                    print(e)
                    df_stock = spider_bijiewang.main(today)
                    print("爬取完成", datetime.now())
                try:
                    # 三个功能的流程函数
                    process(df_stock, today, now_hour, now_minute)
                except Exception as e:
                    print(e)
            except Exception as e:
                # 当两个网站都爬取失败才会到这
                print(f"{e}", "第二个网站爬取失败")
                # 如果两个网站都爬取失败就发邮件给自己
                try:
                    send_email.send(str(e) + "两个网站都爬取失败", "13086397065@163.com")
                except:
                    print("网络断开连接")
                # 读取总表的数据
                if now_minute == 0 and now_hour == 0:
                    df_all_stock = pd.read_csv(
                        '../all_stock/stock' + (today - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv')
                else:
                    df_all_stock = pd.read_csv('../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
                # 复制df_all_stock最后一列数据,总表每一列的列名是时间,且相邻列的时间相差5分钟
                df_stock = df_all_stock.iloc[:, [0, -1]].copy()
                # 将df_stock的列名改为当前时间
                df_stock.rename(columns={df_stock.columns[1]: today_str}, inplace=True)
                # 重新走一遍流程函数
                try:
                    process(df_stock, today, now_hour, now_minute)
                except Exception as E:
                    print(f"出现错误：{E}")
        print("所有功能完成：", datetime.now())
        if int(time.strftime("%M")) == now_minute:
            time.sleep(60 - int(time.strftime("%S")))


if __name__ == "__main__":
    cycle_five_minutes()
