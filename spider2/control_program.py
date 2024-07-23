# _*_ coding: UTF-8 _*_
# @Time : 2024/6/5 20:26
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : control_program.py
# @Project : stockSpiderAndstockMonitor
import send_email
import spider_bijiewang
import spider_Bi123
import time
import monitor_function
from datetime import datetime, timedelta
import read_write_csv
import pandas as pd

thirty_day = [0, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17, -16, -15, -14, -13, -12, -11,
              -10, -9, -8, -7, -6, -5, -4, -3, -2, -1]
# 爬取失败：-1 ； 功能1：1 ； 功能2：2 ； 功能3：3 ； 功能4：4 ；功能5：
function_list = {-1: "爬取失败", 1: "功能1", 2: "功能2", 3: "功能3", 4: "功能4", 2.1: "功能2.1（8点运行的功能2）",
                 3.1: "功能3.1（8点运行的功能3）"}
# , '3145971793@qq.com'
all_send_email_addr = ['13086397065@163.com']
test_send_email_addr = ['13086397065@163.com', '1410959463@qq.com']


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


# 存入总表
def write_all_stock_to_csv(df_stock, today, now_minute, now_hour):
    if now_minute == 0 and now_hour == 0:
        # 读取旧数据
        old_all_stock = read_write_csv.read_data_from_csv(
            '../all_stock/stock' + (today - timedelta(days=1)).strftime('%Y-%m-%d') + '.csv')
        # 拼接新数据并填补缺失值
        new_all_stock = read_write_csv.handler_data(df_stock, old_all_stock)
        # 将数据存入前一天的总表
        read_write_csv.write_all_stock_to_csv(new_all_stock,
                                              '../all_stock/stock' + (today - timedelta(days=1)).strftime(
                                                  '%Y-%m-%d') + '.csv')
        # 获取最新的数据
        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
        # 将新数据存入新的总表中
        read_write_csv.write_all_stock_to_csv(df_stock, '../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
    else:
        # 读取旧数据
        old_all_stock = read_write_csv.read_data_from_csv('../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
        # 拼接新数据并填补缺失值
        new_all_stock = read_write_csv.handler_data(df_stock, old_all_stock)
        # 获取最新的数据
        df_stock = new_all_stock.iloc[:, [0, -1]].copy()
        # 将新数据存入总表中
        read_write_csv.write_all_stock_to_csv(new_all_stock, '../all_stock/stock' + today.strftime('%Y-%m-%d') + '.csv')
    print("总表存储完成", datetime.now())
    return new_all_stock, df_stock


# 存入小时表中
def write_all_stock_one_hour_to_csv(df_stock, today, now_hour):
    # 将每支股票存入分表
    if now_hour == 0:
        # 读取旧数据
        old_one_hour_all_stock = read_write_csv.read_data_from_csv(
            '../all_stock_one_hour/' + (today - timedelta(days=1)).strftime("%Y-%m-%d") + '.csv')
        # 拼接数据并填补缺失值
        new_one_hour_all_stock = read_write_csv.handler_data(df_stock, old_one_hour_all_stock)
        # 将新数据存入表中
        read_write_csv.write_all_stock_to_csv(new_one_hour_all_stock,
                                              '../all_stock_one_hour/' + (today - timedelta(days=1)).strftime(
                                                  "%Y-%m-%d") + '.csv')
        read_write_csv.write_all_stock_to_csv(df_stock,
                                              '../all_stock_one_hour/' + today.strftime("%Y-%m-%d") + '.csv')
    else:
        # 读取旧数据
        old_one_hour_all_stock = read_write_csv.read_data_from_csv(
            '../all_stock_one_hour/' + today.strftime("%Y-%m-%d") + '.csv')
        # 拼接数据并填补缺失值
        new_one_hour_all_stock = read_write_csv.handler_data(df_stock, old_one_hour_all_stock)
        # 将新数据存入表中
        read_write_csv.write_all_stock_to_csv(new_one_hour_all_stock,
                                              '../all_stock_one_hour/' + today.strftime("%Y-%m-%d") + '.csv')
    # 小时数据存储完成
    print("小时分表存储完成", datetime.now())
    if new_one_hour_all_stock.shape[1] < 3:
        return None
    else:
        return new_one_hour_all_stock.iloc[:, [0, -2, -1]].copy()


# 将收盘价存入收盘价表中,并返回收盘价表中的数据
def write_all_stock_one_day_to_csv(df_stock, today, now_hour=""):
    """

    :param df_stock:
    :param today: 该函数都是在0点启动，传入的today是0点时间，例如现在是2024-06-07 00:00
    :param now_hour: 字符串
    :return:
    """
    yesterday = today - timedelta(days=1)
    # 将昨天的日期格式化为字符串
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    df_stock.rename(columns={today.strftime("%Y-%m-%d %H:%M"): yesterday_str}, inplace=True)
    # 读取旧数据
    closing_all_stock = read_write_csv.read_data_from_csv('../closing_price/closing_price' + now_hour + '.csv')
    # 拼接新旧数据
    closing_all_stock = read_write_csv.handler_data(df_stock, closing_all_stock)
    # 存入收盘价表中（0点收盘）
    read_write_csv.write_all_stock_to_csv(closing_all_stock, '../closing_price/closing_price' + now_hour + '.csv')
    return closing_all_stock


def send_email_and_record(text, function_id, email_subject):
    # 发送邮件
    send_email.do_send_mail(text, all_send_email_addr, email_subject)
    # 记录发送邮件次数
    monitor_function.record_today_send_email_state(text, function_id)


# 流程函数
def process(df_stock, today, now_hour, now_minute):
    """

    :param df_stock:
    :param today:
    :param now_hour:
    :param now_minute:
    :return:
    """
    # new_all_stock: 总表中的数据；df_stock: 当前时刻的股票数据
    new_all_stock, df_stock = write_all_stock_to_csv(df_stock, today, now_hour, now_minute)
    # 当前分钟为0
    if now_minute == 0:
        try:
            # name_price1_price2: 最近2小时的数据；df_stock: 当前时刻的股票数据
            # 存数据进总表，并传回最近两小时的股票数据
            name_price1_price2 = write_all_stock_one_hour_to_csv(df_stock, today, now_hour)
            if name_price1_price2 is not None:
                # 功能1，传入最近两小时的价格和当前小时的小时数
                text = monitor_function.function1(name_price1_price2, now_hour)
                # 后面用不到这个变量，将它赋值为Null
                name_price1_price2 = None
                send_email_and_record(text, 1, "异常1")
            print("第一个功能完成", datetime.now())
        except Exception as e:
            print(e)
        # 如果当前小时为0，则进行寻找和监测
        if now_hour == 0:
            # 使用timedelta来减去一天，得到昨天的日期
            yesterday = today - timedelta(days=1)
            # 将数据存入收盘价表中，并返回所有收盘价
            closing_all_stock = write_all_stock_one_day_to_csv(df_stock, today)
            if closing_all_stock.shape[1] >= 3:
                name_closing_price1_closing_price2 = closing_all_stock.iloc[:, [0, -2, -1]].copy()
                name_closing_price1_closing_price2_percentage = monitor_function.calculate_rise_fall_percentage(
                    name_closing_price1_closing_price2)
                try:
                    if closing_all_stock.shape[1] < 31:
                        thirty_day_closing_price = closing_all_stock.copy()
                    else:
                        # 读取最后30天的收盘价
                        thirty_day_closing_price = closing_all_stock.iloc[:, thirty_day].copy()
                    closing_all_stock = None
                    text = monitor_function.function2(name_closing_price1_closing_price2_percentage,
                                                      monitor_function.search_max_closing_price(
                                                          thirty_day_closing_price))
                    send_email_and_record(text, 2, "异常2")
                except Exception as e:
                    print(e)
            print("功能2完成", datetime.now())
            name_max_min_open_amplitude_percentage = monitor_function.calculate_amplitude(new_all_stock)
            name_max_min_open_amplitude_percentage.to_csv('../amplitude/' + yesterday.strftime("%Y-%m-%d") + '.csv',
                                                          index=False)
            try:
                text = monitor_function.function3(name_max_min_open_amplitude_percentage, yesterday)
                send_email_and_record(text, 3, "异常3")
            except Exception as e:
                print(e)
            print("功能3完成", datetime.now())
            try:
                text = monitor_function.function4(name_max_min_open_amplitude_percentage, yesterday)
                send_email_and_record(text, 4, "异常4")
            except Exception as e:
                print(e)
            print("功能4完成", datetime.now())
            # 统计今天发送邮件的情况
            text = monitor_function.count_today_send_email(function_list, yesterday.strftime("%Y-%m-%d"))
            # 发送邮件
            send_email.do_send_mail(text, all_send_email_addr, "今日发送邮件情况")
            print("统计发送邮件情况完成", datetime.now())
        # 如果当前小时为8，则进行寻找和监测
        if now_hour == 8:
            # 使用timedelta来减去一天，得到昨天的日期
            yesterday = today - timedelta(days=1)
            try:
                closing_all_stock = write_all_stock_one_day_to_csv(df_stock, today, str(now_hour))
                if closing_all_stock.shape[1] >= 3:
                    name_closing_price1_closing_price2 = closing_all_stock.iloc[:, [0, -2, -1]].copy()
                    name_closing_price1_closing_price2_percentage = monitor_function.calculate_rise_fall_percentage(
                        name_closing_price1_closing_price2)
                    try:
                        if closing_all_stock.shape[1] < 31:
                            thirty_day_closing_price = closing_all_stock.copy()
                        else:
                            # 读取最后30天的收盘价
                            thirty_day_closing_price = closing_all_stock.iloc[:, thirty_day].copy()
                        closing_all_stock = None
                        text = monitor_function.function2(name_closing_price1_closing_price2_percentage,
                                                          monitor_function.search_max_closing_price(
                                                              thirty_day_closing_price))
                        send_email_and_record(text, 2.1, "异常2，国际事件")
                    except Exception as e:
                        print(e)
                print("功能2.1完成", datetime.now())
                try:
                    yesterday_new_all_stock = read_write_csv.read_data_from_csv(
                        '../all_stock/stock' + yesterday.strftime("%Y-%m-%d") + '.csv')
                    yesterday_new_all_stock = yesterday_new_all_stock.drop(
                        columns=yesterday_new_all_stock.columns[1:97])
                    new_all_stock = new_all_stock.drop(columns=new_all_stock.columns[1])
                    new_all_stock = pd.merge(yesterday_new_all_stock, new_all_stock, on='name')
                    name_max_min_open_amplitude_percentage = monitor_function.calculate_amplitude(new_all_stock)
                    text = monitor_function.function3(name_max_min_open_amplitude_percentage, yesterday,
                                                      str(now_hour))
                    send_email_and_record(text, 3.1, "异常3，国际时间")
                except Exception as e:
                    print(e)
                print("功能3.1完成", datetime.now())
            except Exception as e:
                print(e)
                send_email.send(str(e), test_send_email_addr, "异常")


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
                    send_email.do_send_mail(str(e) + "两个网站都爬取失败", test_send_email_addr, "两个网站都爬取失败")
                    monitor_function.record_today_send_email_state("爬取失败", -1)
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
