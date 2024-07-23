# _*_ coding: UTF-8 _*_
# @Time : 2024/7/8 10:39
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : spider_process.py
# @Project : stockSpiderAndstockMonitor
from datetime import datetime, timedelta
import pandas as pd
import spider_bijiewang
import spider_Bi123
from record_sendemail_state import send_email_and_record


# 启动爬虫的流程控制函数。当爬取失败后就读取上一次的数据充当本次的数据
def spider_process(today, now_hour, now_minute):
    today_str = today.strftime('%Y-%m-%d %H:%M')
    try:
        try:
            df_stock = spider_Bi123.main(today)
            print("爬取完成", datetime.now())
        except Exception as e:
            print(e)
            df_stock = spider_bijiewang.main(today)
            print("爬取完成", datetime.now())
    except Exception as e:
        # 当两个网站都爬取失败才会到这
        print(f"{e}", "第二个网站爬取失败")
        # 如果两个网站都爬取失败就发邮件给自己
        try:
            send_email_and_record(str(e) + "两个网站都爬取失败", -1, "两个网站都爬取失败")
        except:
            print("网络断开连接")
        # 读取总表的数据
        if now_minute == 0 and now_hour == 0:
            df_all_stock = pd.read_csv(
                '../all_stock/stock' + (today - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv')
        else:
            df_all_stock = pd.read_csv('../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
        # 复制df_all_stock最后一列数据,总表每一列的列名是时间,且相邻列的时间相差5分钟
        df_stock = df_all_stock.iloc[:, [0, -1]]
        # 将df_stock的列名改为当前时间
        df_stock = df_stock.rename(columns={df_stock.columns[1]: today_str})
    return df_stock
