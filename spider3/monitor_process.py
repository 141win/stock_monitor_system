# _*_ coding: UTF-8 _*_
# @Time : 2024/7/8 10:30
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : monitor_process.py
# @Project : stockSpiderAndstockMonitor
import pandas as pd
from datetime import datetime, timedelta
import calculate_module as cal
import read_write_csv
from function1 import function1
from function2 import function2
from function3 import function3
from function4 import function4
from function5 import function5
from function6 import function6
from spider4 import record_sendemail_state

# thirty_day = [0, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17, -16, -15, -14, -13, -12, -11,
#               -10, -9, -8, -7, -6, -5, -4, -3, -2, -1]


def process(df_stock, today, now_hour, now_minute):
    new_all_stock, df_stock = read_write_csv.write_all_stock_to_csv(df_stock, today, now_minute, now_hour)
    if now_minute == 0:
        try:

            # name_price1_price2: 最近2小时的数据；df_stock: 当前时刻的股票数据
            # 存数据进小时价总表，并传回最近两小时的股票数据
            name_price1_price2 = read_write_csv.write_all_stock_one_hour_to_csv(df_stock, today, now_hour)
            cal.calculate_ri_fa_per_and_save(name_price1_price2, today)

            if name_price1_price2 is not None:
                # 功能1，传入最近两小时的价格和当前小时的小时数
                function1(today)
            print("第一个功能完成", datetime.now())
        except Exception as e:
            print(e)
        try:
            function5(today)
            print("第五个功能完成", datetime.now())
        except Exception as e:
            print(e)
        try:
            function6(today)
            print("第六个功能完成", datetime.now())
        except Exception as e:
            print(e)
        if now_hour == 0:
            # 使用timedelta来减去一天，得到昨天的日期
            yesterday = today - timedelta(days=1)
            # # 将数据存入收盘价表中，并返回所有收盘价
            # closing_all_stock = read_write_csv.write_all_stock_one_day_to_csv(df_stock, today)
            # if closing_all_stock.shape[1] >= 3:
            #     name_closing_price1_closing_price2 = closing_all_stock.iloc[:, [0, -2, -1]].copy()
            #     name_closing_price1_closing_price2_percentage = cal.calculate_rise_fall_percentage(
            #         name_closing_price1_closing_price2)
            #     try:
            # if closing_all_stock.shape[1] < 31:
            #     thirty_day_closing_price = closing_all_stock.copy()
            # else:
            #     # 读取最后30天的收盘价
            #     thirty_day_closing_price = closing_all_stock.iloc[:, thirty_day].copy()
            # closing_all_stock = None
            # function2(name_closing_price1_closing_price2_percentage,
            #           calculate_module.search_max_closing_price(
            #               thirty_day_closing_price))
            # except Exception as e:
            #     print(e)
            # print("功能2完成", datetime.now())
            cal.calculate_amplitude_and_save(new_all_stock, yesterday)

            try:
                function3(yesterday)
            except Exception as e:
                print(e)
            print("功能3完成", datetime.now())
            try:
                function4(yesterday)
            except Exception as e:
                print(e)
            print("功能4完成", datetime.now())
            # 统计今天发送邮件的情况
            record_sendemail_state.count_today_send_email(yesterday.strftime("%Y-%m-%d"))
            print("统计发送邮件情况完成", datetime.now())
        # 如果当前小时为8，则进行寻找和监测
        if now_hour == 8:
            # 使用timedelta来减去一天，得到昨天的日期
            yesterday = today - timedelta(days=1)
            try:
                # closing_all_stock = read_write_csv.write_all_stock_one_day_to_csv(df_stock, today, str(now_hour))
                # if closing_all_stock.shape[1] >= 3:
                #     name_closing_price1_closing_price2 = closing_all_stock.iloc[:, [0, -2, -1]].copy()
                #     name_closing_price1_closing_price2_percentage = cal.calculate_rise_fall_percentage(
                #         name_closing_price1_closing_price2)
                #     try:
                #         if closing_all_stock.shape[1] < 31:
                #             thirty_day_closing_price = closing_all_stock.copy()
                #         else:
                #             # 读取最后30天的收盘价
                #             thirty_day_closing_price = closing_all_stock.iloc[:, thirty_day].copy()
                #         closing_all_stock = None
                #         function2(name_closing_price1_closing_price2_percentage,
                #                   cal.search_max_closing_price(
                #                       thirty_day_closing_price))
                #     except Exception as e:
                #         print(e)
                # print("功能2.1完成", datetime.now())
                try:
                    yesterday_new_all_stock = read_write_csv.read_data_from_csv(
                        '../all_stock/stock' + yesterday.strftime("%Y-%m-%d") + '.csv')
                    yesterday_new_all_stock = yesterday_new_all_stock.drop(
                        columns=yesterday_new_all_stock.columns[1:97])
                    new_all_stock = new_all_stock.drop(columns=new_all_stock.columns[1])
                    new_all_stock = pd.merge(yesterday_new_all_stock, new_all_stock, on='name')
                    cal.calculate_amplitude_and_save(new_all_stock, yesterday)
                    function3(yesterday)
                except Exception as e:
                    print(e)
                print("功能3.1完成", datetime.now())
            except Exception as e:
                print(e)
