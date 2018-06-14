#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from urllib.parse import urljoin
from requests.exceptions import Timeout, RequestException
import os
import re


import test_tool
from downloader1 import HtmlDownloader
from parser1 import HouseInfoHandler, DealInfoHandler, RegionInfoHandler


MAX_PRICE = 900
MIN_PRICE = 650
BUILD_AGE = {'0-5': 'y1', '0-10': 'y2', '0-15': 'y3', '0-20': 'y4', }
HOUSE_TYPE = {'1': 'l1', '2': 'l2', '3': 'l3', '4': 'l4'}
BOUND = '{}{}bp{}ep{}'.format(BUILD_AGE['0-20'], HOUSE_TYPE['2'], MIN_PRICE, MAX_PRICE)

#各区在售二手房爬取开始页面
l_sale_start_urls = [
    'https://bj.ke.com/ershoufang/dongcheng/pg{}{}'.format(1, BOUND),
    'https://bj.ke.com/ershoufang/xicheng/pg{}{}'.format(1, BOUND),
    'https://bj.ke.com/ershoufang/chaoyang/pg{}{}'.format(1, BOUND),
    'https://bj.ke.com/ershoufang/haidian/pg{}{}'.format(1, BOUND),
    'https://bj.ke.com/ershoufang/fengtai/pg{}{}'.format(1, BOUND),
    'https://bj.ke.com/ershoufang/shijingshan/pg{}{}'.format(1, BOUND),
    'https://bj.ke.com/ershoufang/tongzhou/pg{}{}'.format(1, BOUND),
    'https://bj.ke.com/ershoufang/changping/pg{}{}'.format(1, BOUND)
]
#各区下属区片名称爬取
dict_district_start_url = {
    u'东城': 'https://bj.ke.com/xiaoqu/dongcheng/',
    u'西城': 'https://bj.ke.com/xiaoqu/xicheng/',
    u'朝阳': 'https://bj.ke.com/xiaoqu/chaoyang/',
    u'海淀': 'https://bj.ke.com/xiaoqu/haidian/',
    u'丰台': 'https://bj.ke.com/xiaoqu/fengtai/',
    u'石景山': 'https://bj.ke.com/xiaoqu/shijingshan/',
    u'通州': 'https://bj.ke.com/xiaoqu/tongzhou/',
    u'昌平': 'https://bj.ke.com/xiaoqu/changping/'
}
#各区历史成交二手房爬取
l_deal_start_urls = [
    'https://bj.ke.com/chengjiao/dongcheng/',
    'https://bj.ke.com/chengjiao/xicheng/',
    'https://bj.ke.com/chengjiao/chaoyang/',
    'https://bj.ke.com/chengjiao/haidian/',
    'https://bj.ke.com/chengjiao/fengtai/',
    'https://bj.ke.com/chengjiao/shijingshan/',
    'https://bj.ke.com/chengjiao/tongzhou/',
    'https://bj.ke.com/chengjiao/changping/'
]


def prepare():
    proj_path = os.path.abspath('..')
    path = os.path.join(proj_path, 'result')
    if not os.path.isdir(path):
        os.makedirs(path)

def get_2_level_data(starturl, handler):
    nexturl = starturl
    while True:
        html1 = HtmlDownloader().download_html(nexturl)
        nexturl, l_l2_data_url = handler.parse_l1_data(html1)
        for i in l_l2_data_url:
            html2 = HtmlDownloader().download_html(i)
            m_row = handler.parse_l2_data(html2)
            handler.persist(m_row)
        if nexturl is None:
            break

def get_1_level_data(starturl, handler):
    html1 = HtmlDownloader().download_html(starturl)
    next_url, m_row = handler.parse_l1_data(html1)
    handler.persist(m_row)
    return next_url

#main of this proj
def lianjia_spider_dispatcher():
    prepare()
    for i in l_deal_start_urls:
        get_2_level_data(i, DealInfoHandler())
    for i in l_sale_start_urls:
        get_2_level_data(i, SaleInfoHandler())
    for i in dict_district_start_url:
        get_1_level_data(dict_district_start_url[i], RegionInfoHandler())

if __name__ == '__main__':
    lianjia_spider_dispatcher()