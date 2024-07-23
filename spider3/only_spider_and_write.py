# _*_ coding: UTF-8 _*_
# @Time : 2024/6/3 16:27
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : only_spider_and_write.py
# @Project : stockSpiderAndstockMonitor
import send_email
import spider_bijiewang
import spider_Bi123
import time
from datetime import datetime, timedelta
import pandas as pd
import read_write_csv


# 此程序用于替换代码时使用，只爬取数据和存数据进入总表

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
                    # 将数据存入总表
                    # 将股票数据存入总表，并传回所有股票的所有数据
                    if now_minute == 0 and now_hour == 0:
                        # 读取旧数据
                        old_all_stock = read_write_csv.read_data_from_csv(
                            '../all_stock/stock' + (today - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv')
                        # 拼接新数据并填补缺失值
                        new_all_stock = read_write_csv.handler_data(df_stock, old_all_stock)
                        # 获取最新的数据
                        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
                        # 将新数据存入新的总表中
                        read_write_csv.write_all_stock_to_csv(df_stock, '../all_stock/stock' + today.strftime(
                            '%Y-%m-%d') + '.csv')
                    else:
                        # 读取旧数据
                        old_all_stock = read_write_csv.read_data_from_csv(
                            '../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
                        # 拼接新数据并填补缺失值
                        new_all_stock = read_write_csv.handler_data(df_stock, old_all_stock)
                        # 获取最新的数据
                        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
                        # 将新数据存入总表中
                        read_write_csv.write_all_stock_to_csv(new_all_stock, '../all_stock/stock' + today.strftime(
                            '%Y-%m-%d') + '.csv')
                    print("总表存储完成", datetime.now())
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
                    # 将数据存入总表
                    # 将股票数据存入总表，并传回所有股票的所有数据
                    if now_minute == 0 and now_hour == 0:
                        # 读取旧数据
                        old_all_stock = read_write_csv.read_data_from_csv(
                            '../all_stock/stock' + (today - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv')
                        # 拼接新数据并填补缺失值
                        new_all_stock = read_write_csv.handler_data(df_stock, old_all_stock)
                        # 获取最新的数据
                        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
                        # 将新数据存入新的总表中
                        read_write_csv.write_all_stock_to_csv(df_stock, '../all_stock/stock' + today.strftime(
                            '%Y-%m-%d') + '.csv')
                    else:
                        # 读取旧数据
                        old_all_stock = read_write_csv.read_data_from_csv(
                            '../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
                        # 拼接新数据并填补缺失值
                        new_all_stock = read_write_csv.handler_data(df_stock, old_all_stock)
                        # 获取最新的数据
                        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
                        # 将新数据存入总表中
                        read_write_csv.write_all_stock_to_csv(new_all_stock, '../all_stock/stock' + today.strftime(
                            '%Y-%m-%d') + '.csv')
                    print("总表存储完成", datetime.now())
                except Exception as E:
                    print(f"出现错误：{E}")
        print("所有功能完成：", datetime.now())
        if int(time.strftime("%M")) == now_minute:
            time.sleep(60 - int(time.strftime("%S")))


if __name__ == "__main__":
    cycle_five_minutes()
