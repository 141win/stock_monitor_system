# _*_ coding: UTF-8 _*_
# @Time : 2024/5/10 17:26
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : spider_Bi123.py
# @Project : stockSpiderAndstockMonitor

import random
import time
import requests
import pandas as pd

# 将需要爬取的url存入列表中
url = 'https://www.bi123.co/crypto-web/open/markets/spot/list'
# 设置多个user_agent,每次随机抽取一个作为header，防反爬
user_agent = ["Mozilla/5.0 (Windows NT 10.0; WOW64)", 'Mozilla/5.0 (Windows NT 6.3; WOW64)',
              'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
              'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
              'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
              'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
              'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
              'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
              'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
              'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
              'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0',
              'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
              'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
              'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
              'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
              'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
              'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
              'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
              'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
              'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
              'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
              'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11']


# 模拟浏览器发送请求下载股票网站的html文档
def get_html(page):
    headers = {
        'User-Agent': random.choice(user_agent)
    }
    param = {
        'baseAsset': "",
        'contractType': "",
        # page是传入的爬取的页号
        'current': page,
        'exchange': "",
        'groupId': 0,
        'index': "",
        'isAll': True,
        'isSelect': False,
        'isSubs': False,
        'metrics': [],
        'periodType': "",
        'plate': "",
        'plates': [],
        'quoteAsset': "",
        'searchText': "",
        'size': 50,
        'sortName': "marketCap",
        'sortOrder': "desc",
        'symbol': "",
        'tagsId': ""
    }
    # 设置响应超时时间
    timeout = 15
    try:
        response = requests.post(url, headers=headers, json=param, timeout=timeout)
        # 检查请求是否成功
        if response.status_code == 200:
            # 请求成功，解析响应内容
            data = response.json()  # 响应为json格式，使用response.json()解析
            return data
        else:
            # 请求失败，打印错误信息
            print(f"Request failed with status code {response.status_code}")
            print(response.text)  # 打印响应的原始文本内容，可能包含错误信息
    except requests.exceptions.RequestException as e:
        # 捕获requests库抛出的所有异常，并打印异常信息
        print(f"发生请求错误：{e}")
        raise


# 将爬取的数据存入DataFrame中
# df1：传入的DataFrame
# sdata：爬取的数据
def put_data_to_dataframe(df1, sdata):
    # 替换头
    # 提取json中部分需要的数据
    data = sdata['data']['records']
    # 将json中值为列表的替换为空
    for i in range(0, 50):
        data[i]['enTagIds'] = None
        data[i]['exchangeList'] = None
        data[i]['kline'] = None
        data[i]['tagIds'] = None
    # 将数据存入DataFrame中
    df = pd.DataFrame(data, columns=['baseAsset', 'price'])
    df.rename(columns={'baseAsset': 'name'}, inplace=True)
    # 将新数据与老数据连接
    if df1.shape[0] == 0:
        df1 = df
    else:
        df1 = pd.concat([df1, df])
    return df1


# today 当前开始爬取的日期时间
def main(today):
    i = 1
    df = pd.DataFrame(columns=['name', 'price'])
    for page in range(1, 16):
        df = put_data_to_dataframe(df, get_html(page))
        print(i)
        i += 1
        # 每爬一页股票休眠0.8秒
        time.sleep(0.8)
    today_str = today.strftime("%Y-%m-%d %H:%M")
    # 将price列中所有数据只保留数字
    df['price'].apply(lambda x: float(str(x).replace(",", "")))
    # 将price列数据类型换为float
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    # 将df price列列名更改为当前开始爬取的时间
    df.rename(columns={'price': today_str}, inplace=True)
    return df
