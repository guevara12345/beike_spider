#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import os
from datetime import date
import time
from lxml import etree
from urllib.parse import urljoin


from db_orm import DealInfo, RegionInfo, SaleInfo, DBSession
import config


proj_path = os.path.abspath('..')


class RegionInfoHandler:
    def parse_l1_data(self, html, url):
        selector = etree.HTML(html.encode('utf-8'))
        l_region = \
            [str(x).strip() for x in selector.xpath("//div[@data-role='ershoufang']/div[2]/a/text()")]
        list_r = []
        for i in l_region:
            r = RegionInfo()
            r.district = selector.xpath(
                "//div[@data-role='ershoufang']/div[1]/a[@class='selected'][1]/text()")[0]
            r.region = i
            if i in config.chaoyang_far_region:
                r.is_too_far = 1
            else:
                r.is_too_far = 0
            list_r.append(r)
        print('parse {}, return {} items of region data'.format(url, len(list_r)))
        return None, list_r

    def persist(self, list_r):
        try:
            session = DBSession()
            for i in list_r:
                session.merge(i)
            session.commit()
            print('persist {} items of region data'.format(len(list_r)))
        except Exception as e:
            session.roolback()
            print(e)
        finally:
            session.close()
        return False


class SaleInfoHandler:
    def clear_table(self):
        try:
            session = DBSession()
            session.query(SaleInfo).filter().update({SaleInfo.is_expire: 1})
            session.commit()
            print('clear done')
        except Exception as e:
            session.rollback()
            print(e)
        finally:
            session.close()

    def parse_l1_data_2_urls(self, html, url):
        selector = etree.HTML(html.encode('utf-8'))
        l_next_level_urls = \
            [str(x) for x in selector.xpath(
                r"//div[@class='leftContent']//ul[@class='sellListContent']//li[@class='clear']//div[@class='title']/a/@href")]
        # next_page
        next_page = None
        page_data = selector.xpath(r"//div[@class='page-box house-lst-page-box'][1]/@page-data")[0]
        mobj = re.match(r'{"totalPage":(\d+),"curPage":(\d+)}', page_data)
        curPage = int(mobj[2])
        totalPage = int(mobj[1])
        page_url = selector.xpath(r"//div[@class='page-box house-lst-page-box'][1]/@page-url")[0]
        if curPage + 1 <= totalPage:
            next_page = urljoin('https://bj.ke.com/', str(page_url).format(page=curPage + 1))
        print('parse l1 sale data of {} done\n\tnext_page = {}\n\tlen(l_next_level_urls) = {}'.
              format(url, next_page, len(l_next_level_urls)))
        return next_page, l_next_level_urls

    def parse_l2_data_2_persist(self, html, url):
        selector = etree.HTML(html.encode('utf-8'))
        r = SaleInfo()
        r.code = str(selector.xpath(
            r"//div[@class='overview']/div[@class='content']//div[@class='houseRecord']/span[2]/text()")[0]).strip()
        if selector.xpath(r"//div[@class ='title-wrapper']//h1/span"):
            r.is_expire = 1
        else:
            r.is_expire = 0
            r.total_price = str(selector.xpath(
                r"//div[@class='overview']//div[@class='price ']//span[@class='total']/text()")[0]).strip()
            r.unit_price = str(selector.xpath(
                r"//div[@class='overview']//div[@class='price ']//span[@class='unitPriceValue']/text()")[0]).strip()
            # 房屋户型
            r.room = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[1]/text()")[0]).strip()
            # 所在楼层
            r.floor = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[2]/text()")[0]).strip()
            # 建筑面积
            r.build_area = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[3]/text()")[0]).strip()
            # 户型结构
            r.huxing = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[4]/text()")[0]).strip()
            # 套内面积
            r.house_area = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[5]/text()")[0]).strip()
            # 建筑类型
            r.building_type = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[6]/text()")[0]).strip()
            # 房屋朝向
            r.orientations = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[7]/text()")[0]).strip()
            # 建筑结构
            r.buiding_texture = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[8]/text()")[0]).strip()
            #装修情况
            r.decoration = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[9]/text()")[0]).strip()
            #梯户比例
            r.elevator_house_proportion = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[10]/text()")[0]).strip()
            #供暖方式
            r.heating = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[11]/text()")[0]).strip()
            #配备电梯
            r.is_elevator_exist = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[12]/text()")[0]).strip()
            #产权年限
            r.property_right = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[13]/text()")[0]).strip()
            #####################################################################################
            #挂牌时间
            r.guapai_time = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[1]/span[2]/text()")[0]).strip()
            #交易权属
            r.property_type = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[2]/span[2]/text()")[0]).strip()
            #上次交易
            r.last_deal_time = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[3]/span[2]/text()")[0]).strip()
            #房屋用途
            r.house_usage = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[4]/span[2]/text()")[0]).strip()
            #房屋年限
            r.deal_year = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[5]/span[2]/text()")[0]).strip()
            #产权所属
            r.property_ownership = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[6]/span[2]/text()")[0]).strip()
            #抵押信息
            r.mortgage = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[7]/span[2]/text()")[0]).strip()

            r.xiaoqu = str(selector.xpath(
                r"//div[@class='overview']//div[@class='aroundInfo']/div[@class='communityName']/a[1]/text()")[0]).strip()
            r.region = str(selector.xpath(
                r"//div[@class='overview']//div[@class='aroundInfo']/div[@class='areaName']//a[2]/text()")[0]).strip()
            r.district = str(selector.xpath(
                r"//div[@class='overview']//div[@class='aroundInfo']/div[@class='areaName']//a[1]/text()")[0]).strip()

            r.url = r'https://bj.ke.com/ershoufang/{code}.html'.format(code = r.code)
            r.price_change = 0
        print('parse l2 sale data of {} done'.format(url))
        return r

    def persist(self, list_r):
        try:
            session = DBSession()
            is_stop = None
            for i in list_r:
                if i.is_expire is 0:
                    l_stored_data = session.query(SaleInfo).filter(SaleInfo.code == i.code).all()
                    if l_stored_data:
                        i.price_change = i.total_price - l_stored_data[0].total_price
                    session.merge(i)
                else:
                    session.query(SaleInfo).filter(SaleInfo.code == i.code).\
                        update({SaleInfo.is_expire: 1})
            session.commit()
            print('persist {} items of data, and is_stop = {}'.format(len(list_r), True if is_stop else False))
        except Exception as e:
            session.rollback()
            print(e)
        finally:
            session.close()
        return True if is_stop else False


class DealInfoHandler:

    def parse_l1_data_2_urls(self, html, url):
        l_url = None
        try:
            selector = etree.HTML(html.encode('utf-8'))
            l_add_url = selector.xpath(r"//div[@data-role='ershoufang']/div[2]/a/@href")
            l_url = [urljoin(r'https://bj.ke.com/chengjiao/', str(x).strip()) for x in l_add_url]
            #l_url = [str(x).strip() for x in l_add_url]
            print('parse l1 deal data of {} done, return {} urls'.format(url, len(l_url)))
        except Exception as e:
            print(e)
        return l_url

    def parse_l2_data_2_urls(self, html, url):
        next_page = None
        l_next_level_urls = None
        try:
            selector = etree.HTML(html.encode('utf-8'))
            l_next_level_urls = \
                [str(x) for x in selector.xpath(r"//div[@class='leftContent']//ul[@class='listContent']//li/a/@href")]
            #next_page
            page_data = selector.xpath(r"//div[@class='page-box house-lst-page-box'][1]/@page-data")[0]
            mobj = re.match(r'{"totalPage":(\d+),"curPage":(\d+)}', page_data)
            curPage = int(mobj[2])
            totalPage = int(mobj[1])
            page_url = selector.xpath(r"//div[@class='page-box house-lst-page-box'][1]/@page-url")[0]
            if curPage+1 <= totalPage:
                next_page = urljoin('https://bj.ke.com/', str(page_url).format(page = curPage+1))
            print('parse l2 deal data of {} done, next_page is {}'.format(url, next_page))
        except Exception as e:
            print(e)
        return next_page, l_next_level_urls

    def parse_l3_data_2_persist(self, html, url):
        r = DealInfo()
        try:
            selector = etree.HTML(html.encode('utf-8'))
            if selector.xpath(r"//div[@class='overview']//div[@class='price']//i[1]"):
                r.total_price = str(selector.xpath(
                    r"//div[@class='overview']//div[@class='price']//i[1]/text()")[0]).strip()
            else:
                r.total_price = 'No Data'
            if selector.xpath(r"//div[@class='overview']//div[@class='price']//b[1]"):
                r.unit_price = str(selector.xpath(
                    r"//div[@class='overview']//div[@class='price']//b[1]/text()")[0]).strip()
            else:
                r.unit_price = 'No Data'

            #房屋户型
            r.room = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[1]/text()")[0]).strip()
            #所在楼层
            r.floor = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[2]/text()")[0]).strip()
            #建筑面积
            r.build_area = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[3]/text()")[0]).strip()
            #户型结构
            r.huxing = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[4]/text()")[0]).strip()
            #套内面积
            r.house_area = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[5]/text()")[0]).strip()
            #建筑类型
            r.building_type = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[6]/text()")[0]).strip()
            #房屋朝向
            r.orientations = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[7]/text()")[0]).strip()
            #建成年代
            r.buiding_time = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[8]/text()")[0]).strip()
            #装修情况
            r.decoration = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[9]/text()")[0]).strip()
            #建筑结构
            r.buiding_texture = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[10]/text()")[0]).strip()
            #供暖方式
            r.heating = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[11]/text()")[0]).strip()
            #梯户比例
            r.elevator_house_proportion = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[12]/text()")[0]).strip()
            #产权年限
            r.property_time = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[12]/text()")[0]).strip()
            #配备电梯
            r.is_elevator = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='base']//li[14]/text()")[0]).strip()

            #链家编号
            r.code = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[1]/text()")[0]).strip()
            # 交易权属
            r.property_type = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[2]/text()")[0]).strip()
            #挂牌时间
            r.guapai_time = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[3]/text()")[0]).strip()
            #房屋用途
            r.house_usage = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[4]/text()")[0]).strip()
            #房屋年限
            r.deal_year = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[5]/text()")[0]).strip()
            #房权所属
            r.property_ownership = str(selector.xpath(
                r"//div[@class='introContent']//div[@class='transaction']//li[6]/text()")[0]).strip()
            #region
            str_region = str(selector.xpath(r"//div[@class='deal-bread']//a[last()]/text()")[0]).strip()
            mobj = re.match(r'(\S+)二手房成交价格', str_region)
            if mobj:
                r.region = mobj[1]
            #district
            str_district = str(selector.xpath(r"//div[@class='deal-bread']//a[last()-1]/text()")[0]).strip()
            mobj = re.match(r'(\S+)二手房成交价格', str_district)
            if mobj:
                r.district = mobj[1]
            #xiaoqu
            str_xiaoqu = str(selector.xpath(r"//div[4]/div[@class='wrapper']/text()")[0]).strip()
            mobj = re.match(r'(\S+)\s+(\S+)\s+(\S+)', str_xiaoqu)
            if mobj:
                r.xiaoqu = mobj[1]
            #deal_time
            str_deal_time = str(selector.xpath(r"//div[4]/div[@class='wrapper']/span/text()")[0]).strip()
            mobj = re.match(r'(\S+)\s+(\S+)', str_deal_time)
            if mobj:
                r.deal_time = mobj[1].replace('.','-')

            r.is_expire = 0
            r.url = url
            print('parse l3 deal data of {} done'.format(url))
        except Exception as e:
            print(e)
        return r

    def persist(self, list_r):
        try:
            session = DBSession()
            is_stop = None
            #is_stop = session.query(DealInfo).filter(DealInfo.code == list_r[0].code).one_or_none()
            for i in list_r:
                session.merge(i)
            session.commit()
            print('persist {} items of deal data, and is_stop = {}'.format(len(list_r), True if is_stop else False))

        except Exception as e:
            session.roolback()
            print(e)
        finally:
            session.close()
        return True if is_stop else False


if __name__ == '__main__':
    #sale info test
    with open(os.path.join(proj_path, u'data/deal0_1.html'), 'r') as f:
        DealInfoHandler().parse_l1_data_2_urls(f.read(), 'test')
    with open(os.path.join(proj_path, u'data/deal1_2.html'), 'r') as f:
        DealInfoHandler().parse_l2_data_2_urls(f.read(), 'test')
    with open(os.path.join(proj_path, u'data/deal_level_2_2.html'), 'r') as f:
        DealInfoHandler().parse_l3_data_2_persist(f.read(), 'test')
    #region info test
    with open(os.path.join(proj_path, u'data/region_1.html'), 'r') as f:
        RegionInfoHandler().parse_l1_data(f.read(), 'text')
    #sale info test
    '''
    with open(os.path.join(proj_path, u'data/sale1_1.html'), 'r') as f:
        SaleInfoHandler().parse_l1_data_2_urls(f.read(), 'test')
    '''
    with open(os.path.join(proj_path, u'data/sale0_2.html'), 'r') as f:
        SaleInfoHandler().parse_l1_data_2_urls(f.read(), 'last')
    with open(os.path.join(proj_path, u'data/sale_on.html'), 'r') as f:
        r = SaleInfoHandler().parse_l2_data_2_persist(f.read(), 'on')
    with open(os.path.join(proj_path, u'data/sale_xiajia.html'), 'r') as f:
        r = SaleInfoHandler().parse_l2_data_2_persist(f.read(), 'xiajia')
    print('OK')