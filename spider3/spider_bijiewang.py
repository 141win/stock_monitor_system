# _*_ coding: UTF-8 _*_
# @Time : 2024/5/10 17:25
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : spider_bijiewang.py
# @Project : stockSpiderAndstockMonitor

import random
import socket
import time
import pandas as pd
from bs4 import BeautifulSoup
import bs4
import urllib.request
import urllib.parse
import re

# # 将需要爬取的url存入列表中
url = 'https://www.528btc.com/e/extend/api/index.php?m=v2&c=coinlist'
ulist = []
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
        'User-Agent': random.choice(user_agent)}

    # post请求参数
    param = {
        'm': 'v2',
        'c': 'coinlist',
        'page': str(page)

    }

    # post请求参数需要进行两次编码，第一次urlencode：对字典参数进行Unicode编码转成字符串，第二次encode：将字符串数据转换为字节类型
    param = urllib.parse.urlencode(param).encode('utf-8')

    # post定制请求可以使用位置传参
    timeout = 10  # 设置响应超时时间
    request = urllib.request.Request(url, param, headers)

    try:
        response = urllib.request.urlopen(request, timeout=timeout)
        # 解码读取数据
        page = response.read().decode('utf-8')
        return page

    except urllib.error.URLError as e:
        # 如果是超时异常，打印出超时信息
        if isinstance(e.reason, socket.timeout):
            print("请求超时！")
        else:
            print(f"发生错误：{e.reason}")
        raise
    except Exception as e:
        # 捕获其他类型异常，并打印异常信息
        print(f"发生错误：{e}")
        raise


# 将ulist中的数据存入DataFrame中
def put_data_to_dataframe(text, time):
    df = pd.DataFrame(text, columns=['name', time])
    return df


# 解析html文档提取数据，存入ulist
def get_data(html):
    soup = BeautifulSoup(html, "html.parser")
    name, price = None, None
    tbody = soup.find_all('tr')
    for tr in tbody:
        if isinstance(tr, bs4.element.Tag):
            name_str = tr.find('div', class_='detail')
            price_str = tr.find_all('td')[3]
            if name_str is not None:
                # 将中文字符去掉，保留英文
                name = re.sub(r'[\u4e00-\u9fa5]+', '', name_str.string.strip())
            if price_str is not None:
                price = float(price_str.string.strip().replace("$", "").replace(",", ""))
            if name is not None or price is not None:
                ulist.append([name, price])


# now_hour、now_minute 当前开始爬取的时间
def main(today):
    ulist.clear()
    i = 1
    for page in range(1, 36):
        get_data(get_html(page))
        print(i)
        i += 1
        # 每爬一页股票休眠0.8秒
        time.sleep(2)
    today_str = today.strftime("%Y-%m-%d %H:%M")
    df = put_data_to_dataframe(ulist, today_str)
    return df
