#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from requests.exceptions import Timeout, RequestException
import os
import re
import time


import show_result
from downloader1 import HtmlDownloader
from parser1 import SaleInfoHandler, DealInfoHandler, RegionInfoHandler
import config
import log


def prepare():
    path = os.path.join(config.PROJ_PATH, 'result')
    if not os.path.isdir(path):
        os.makedirs(path)

    path = os.path.join(config.PROJ_PATH, 'log')
    if not os.path.isdir(path):
        os.makedirs(path)

def get_3_level_data(starturl, handler):
    html1 = HtmlDownloader().download_html(starturl)
    l_l2_data_url = handler.parse_l1_data_2_urls(html1, starturl)
    for i in l_l2_data_url:
        nexturl_l2 = i
        while True:
            time.sleep(config.SLEEP_TIME)
            html2 = HtmlDownloader().download_html(nexturl_l2)
            nexturl_l2, l_l3_data_url = handler.parse_l2_data_2_urls(html2, nexturl_l2)
            persist_list = []
            for i in l_l3_data_url:
                html3 = HtmlDownloader().download_html(i)
                r = handler.parse_l3_data_2_persist(html3, i)
                if not i is None:
                    persist_list.append(r)
            is_stop = handler.persist(persist_list)
            if nexturl_l2 is None:
                break
            if is_stop:
                break


def get_2_level_data(starturl, handler):
    nexturl = starturl
    while True:
        time.sleep(config.SLEEP_TIME)
        html1 = HtmlDownloader().download_html(nexturl)
        nexturl, l_l2_data_url = handler.parse_l1_data_2_urls(html1, nexturl)
        persist_list=[]
        for i in l_l2_data_url:
            html2 = HtmlDownloader().download_html(i)
            r = handler.parse_l2_data_2_persist(html2, i)
            if not r is None:
                persist_list.append(r)
        is_stop = handler.persist(persist_list)
        if nexturl is None:
            break
        if is_stop:
            break

def get_1_level_data(starturl, handler):
    nexturl = starturl
    while True:
        time.sleep(config.SLEEP_TIME)
        html1 = HtmlDownloader().download_html(nexturl)
        nexturl, list_r = handler.parse_l1_data(html1, nexturl)
        is_stop = handler.persist(list_r)
        if nexturl is None:
            break
        if is_stop:
            break


#main of this proj
def beike_dispacher():
    prepare()
    
    for i in config.dict_district_start_url:
        get_1_level_data(config.dict_district_start_url[i], RegionInfoHandler())
    #show_result.show_sale_info()
    
    SaleInfoHandler().clear_table()
    for i in config.l_sale_start_urls:
        get_2_level_data(i, SaleInfoHandler())
    show_result.show_sale_info()
    '''
    for i in config.l_deal_start_urls:
        get_3_level_data(i, DealInfoHandler())
    '''

if __name__ == '__main__':
    beike_dispacher()
    print('spider has been run in {}'.format(time.strftime("%Y-%b-%d %H:%M:%S")))
    log.info_logger.info('spider has been run in {}'.format(time.strftime("%Y-%b-%d %H:%M:%S")))