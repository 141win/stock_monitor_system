# _*_ coding: UTF-8 _*_
# @Time : 2024/6/30 10:26
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : record_sendemail_state.py
# @Project : stockSpiderAndstockMonitor
# 统计今天发送邮件的情况
import csv
import os
import time
from datetime import datetime, timedelta
from send_email import do_send_mail
import pandas as pd
# import gc

# '3145971793@qq.com'
all_send_email_addr = ['13086397065@163.com']
# 爬取失败：-1 ； 功能1：1 ； 功能2：2 ； 功能3：3 ； 功能4：4 ；功能5：
function_list = {-1: "爬取失败", 1: "功能1", 2: "功能2", 3: "功能3", 4: "功能4", 5: "功能5", 6: "功能6",
                 2.1: "功能2.1（8点运行的功能2）",
                 3.1: "功能3.1（8点运行的功能3）"}


def count_today_send_email(today):
    """
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
            do_send_mail(text, all_send_email_addr, "今日发送邮件情况")
        else:
            return None
    except:
        return None


# 增加发送邮件的情况的记录
def record_today_send_email_state(function_id):
    """
    :param function_id: 发送邮件的功能id
    :return:
    """
    # 如果邮件内容不为空，就将发送邮件的功能id以及发送时间写入csv文件中。
    # if text is not None:
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
        # del df
    else:
        with open(csv_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(data)
            print("写入成功")
    #     del writer, f
    # del now_date
    # del now_str
    # del now_hour
    # del data
    # del csv_path
    # gc.collect()


def send_email_and_record(text, function_id, email_subject):
    if text is not None:
        # 发送邮件
        do_send_mail(text, all_send_email_addr, email_subject)
        # 记录发送邮件次数
        record_today_send_email_state(function_id)
    # del text
    # # del all_send_email_addr
    # del email_subject
    # del function_id
    # gc.collect()
