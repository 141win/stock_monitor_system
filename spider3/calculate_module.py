# _*_ coding: UTF-8 _*_
# @Time : 2024/6/27 12:15
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : calculate_module.py
# @Project : stockSpiderAndstockMonitor

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


def calculate_ri_fa_per_and_save(name_price1_price2, now_time):
    # 计算涨跌幅
    rise_fall_percentage = calculate_rise_fall_percentage(name_price1_price2)
    rise_fall_percentage.to_csv(f'../one_hour_rise_fall/{now_time.strftime("%Y-%m-%d %H")}.csv', index=False)


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


# 找出最大值
def search_max_closing_price(thirty_day_closing_price):
    """
    :param thirty_day_closing_price:
    :return:one_day:有name、max_price、min_price、open_price、amplitude_percentage
    """
    thirty_day_closing_price['max_price'] = thirty_day_closing_price.iloc[:, 1:].max(axis=1)
    thirty_day_closing_price = thirty_day_closing_price.iloc[:, [0, -1]]
    return thirty_day_closing_price


def calculate_amplitude_and_save(one_day_price, now_time):
    amplitude = calculate_amplitude(one_day_price)
    if now_time.hour == 8:
        amplitude.to_csv(f'../amplitude/{now_time.strftime("%Y-%m-%d %H")} 8.csv', index=False)
    else:
        amplitude.to_csv(f'../amplitude/{now_time.strftime("%Y-%m-%d %H")}.csv', index=False)
