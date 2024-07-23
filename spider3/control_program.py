# _*_ coding: UTF-8 _*_
# @Time : 2024/6/27 12:12
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : control_program.py
# @Project : stockSpiderAndstockMonitor

from spider_process import spider_process
import time
from datetime import datetime
from monitor_process import process


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


def cycle_five_minutes():
    while True:
        now_minute = 0
        # 五分钟爬一次，整点爬
        if next_time(5, 0):
            # 获得今天的日期时间
            today = datetime.now()
            now_hour = today.hour
            now_minute = today.minute
            print(today)
            # 启动爬虫流程，获取最新的数据
            df_stock = spider_process(today, now_hour, now_minute)
            # 启动存储计算流程
            try:
                # 功能的流程函数
                process(df_stock, today, now_hour, now_minute)
            except Exception as E:
                print(f"出现错误：{E}")
        print("所有功能完成：", datetime.now())

        if datetime.now().minute == now_minute:
            time.sleep(60 - int(time.strftime("%S")))


if __name__ == "__main__":
    cycle_five_minutes()
