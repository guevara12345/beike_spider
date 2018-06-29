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
        print('parse {}, return {} items of data'.format(url, len(list_r)))
        return None, list_r

    def persist(self, list_r):
        try:
            session = DBSession()
            for i in list_r:
                session.merge(i)
            session.commit()
            print('persist {} items of data'.format(len(list_r)))
        except Exception as e:
            session.roolback()
            print(e)
        finally:
            session.close()
        return False

class SaleInfoHandler:
    # parse https://bj.lianjia.com/ershoufang/haidian/ for max_page_num
    def parse_max_page_num(self, html, path):
        soup = BeautifulSoup(html, 'html.parser')
        p_list = soup.find_all('div', 'page-box house-lst-page-box')
        l = p_list[0].attrs
        m = re.match(r'{"totalPage":(\d+),"curPage":1}', l['page-data'])
        r = int(m.group(1))
        print('parse {}, {} page of html'.format(path, r))
        return r

    def is_exist_in_house_info(self, code):
        db = db_helper.DbExeu()
        exists_sql = '''select * from tb_house_info where code=%s'''
        return db.return_many_with_para(exists_sql, code)

    def persis_house_info(self, dict_house_info):
        if not dict_house_info == None:
            db = db_helper.DbExeu()
            info = self.is_exist_in_house_info(dict_house_info['code'])
            if dict_house_info['is_expire'] == '0':
                insert_sql = '''insert into tb_house_info(code,total_price,unit_price,room,floor, build_area,huxing,house_area,orientations, buiding_texture,decoration, elevator_house_proportion,heating,is_elevator, property_right,building_type, xiaoqu,region,guapai_time, property_type,last_deal_time, house_usage,deal_year,property_ownership,mortgage,is_expire) 
                             values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                update_sql = '''
                        UPDATE tb_house_info SET total_price=%s,
                                 unit_price=%s,
                                 room=%s,
                                 floor=%s,
                                 build_area=%s,
                                 huxing=%s,
                                 house_area=%s,
                                 orientations=%s,
                                 buiding_texture=%s,
                                 decoration=%s,
                                 elevator_house_proportion=%s,
                                 heating=%s,
                                 is_elevator=%s,
                                 property_right=%s,
                                 building_type=%s,
                                 xiaoqu=%s,
                                 region=%s,
                                 guapai_time=%s,
                                 property_type=%s,
                                 last_deal_time=%s,
                                 house_usage=%s,
                                 deal_year=%s,
                                 property_ownership=%s,
                                 mortgage=%s,
                                 is_expire=%s
                        WHERE code=%s'''
                if info[1] == 0:
                    t_inser = tuple([str(dict_house_info[tb_house_info[x]]) for x in range(len(tb_house_info))])
                    db.trans(insert_sql, [t_inser, ])
                else:
                    if not info[0][0][2] == dict_house_info['total_price']:
                        price_change = int(dict_house_info['total_price']) - int(info[0][0][2])
                        datetime = date.today().isoformat()
                        timestamp = int(time.time() * 1000)
                        change_insert = '''
                            insert into tb_price_change(timestamp, code, total_price, price_change, datetime) values (%s, %s, %s, %s, %s)
                        '''
                        insert_date = [(
                                       timestamp, dict_house_info['code'], dict_house_info['total_price'], price_change,
                                       datetime), ]
                        db.trans(change_insert, insert_date)
                        print('{} change {}'.format(dict_house_info['code'], price_change))

                    l_update = []
                    for x in range(len(tb_house_info)):
                        if not x == 0:
                            l_update.append(str(dict_house_info[tb_house_info[x]]))
                    l_update.append(str(dict_house_info[tb_house_info[0]]))
                    db.trans(update_sql, [tuple(l_update), ])
            else:
                if not info[1] == 0:
                    update_sql = '''
                            UPDATE tb_house_info SET is_expire=%s
                            WHERE code=%s'''
                    db.trans(update_sql, [(dict_house_info['is_expire'], dict_house_info['code']), ])
            print('persist data.code = {}'.format(dict_house_info['code']))

    def parse_house_url(self, html, path):
        try:
            list_url = []
            soup = BeautifulSoup(html, 'html.parser')
            l_tag = soup.find_all('div', 'info clear')
            # get house detail info url list
            for i1 in l_tag:
                l = i1.find('div', 'title').a.attrs
                list_url.append(l['href'])
            print('parse {}'.format(path))
            return list_url
        except Exception as e:
            print('pasre {}\nException: {}'.format(path, e))
            raise e

    def parse_house_info(self, html, path):
        print(path)
        try:
            soup = BeautifulSoup(html, 'html.parser')
            t = soup.find('div', 'title-wrapper').find('h1').find('span')
            if not t == None:
                is_expire = t.string
            else:
                is_expire = 0
            dict_house_info = dict()
            overview = soup.find('div', 'overview').find('div', 'content')
            basic = soup.find('div', 'm-content').find('div', 'base')
            transaction = soup.find('div', 'm-content').find('div', 'transaction')
            if is_expire == '已下架':
                dict_house_info['is_expire'] = '1'
                dict_house_info['code'] = overview.find('div', 'houseRecord').find_all('span')[1].contents[0]
            else:
                # fill dict_house_info
                # not下架
                dict_house_info['is_expire'] = '0'
                # code for 房屋代码
                dict_house_info['code'] = overview.find('div', 'houseRecord').find_all('span')[1].contents[0]
                # total_price for 总价
                dict_house_info['total_price'] = overview.find('div', 'price ').find_all('span')[0].string
                # unit_price for 单价
                dict_house_info['unit_price'] = overview.find('div', 'price ').find_all('span')[3].contents[0]
                # room like 两室一厅
                dict_house_info['room'] = basic.find_all('li')[0].contents[1]
                # floor like 顶层(共13层)
                dict_house_info['floor'] = basic.find_all('li')[1].contents[1]
                # biuld_area for 建筑面积
                dict_house_info['build_area'] = basic.find_all('li')[2].contents[1]
                # huxing like 平层
                dict_house_info['huxing'] = basic.find_all('li')[3].contents[1]
                # house_area for 套内面积
                dict_house_info['house_area'] = basic.find_all('li')[4].contents[1]
                # orientations for 朝向
                dict_house_info['orientations'] = basic.find_all('li')[6].contents[1]
                # biuding_texture like 材质
                dict_house_info['buiding_texture'] = basic.find_all('li')[7].contents[1]
                # decoration like 精装
                dict_house_info['decoration'] = basic.find_all('li')[8].contents[1]
                # elevator_house_proportion like 一梯三户
                dict_house_info['elevator_house_proportion'] = basic.find_all('li')[9].contents[1]
                # heating like 自供暖
                dict_house_info['heating'] = basic.find_all('li')[10].contents[1]
                # is_elevator like 有电梯
                dict_house_info['is_elevator'] = basic.find_all('li')[11].contents[1]
                # property_right like 70年
                dict_house_info['property_right'] = basic.find_all('li')[12].contents[1]
                # building_type like 2003年建塔楼
                dict_house_info['building_type'] = overview.find('div', 'area').find('div', 'subInfo').string
                # xiaoqu for 小区
                dict_house_info['xiaoqu'] = overview.find('div', 'communityName').a.string
                # region like 清河
                dict_house_info['region'] = overview.find('div', 'areaName').find_all('a')[1].string

                # guapai_time is 挂牌时间
                dict_house_info['guapai_time'] = transaction.find_all('li')[0].find_all('span')[1].string
                # property_type like 商品房
                dict_house_info['property_type'] = transaction.find_all('li')[1].find_all('span')[1].string
                # last_deal_time is 上次交易
                dict_house_info['last_deal_time'] = transaction.find_all('li')[2].find_all('span')[1].string
                # house_usage is 房屋用途
                dict_house_info['house_usage'] = transaction.find_all('li')[3].find_all('span')[1].string
                # deal_year is 房屋年限
                dict_house_info['deal_year'] = transaction.find_all('li')[4].find_all('span')[1].string
                # property_ownership is 产权所属
                dict_house_info['property_ownership'] = transaction.find_all('li')[5].find_all('span')[1].string
                # mortgage is 抵押信息
                dict_house_info['mortgage'] = transaction.find_all('li')[6].find_all('span')[1].attrs['title']
                print('parse {}'.format(path))
            return dict_house_info
        except AttributeError as e:
            print(e)
            return None


class DealInfoHandler:

    def parse_l1_data_2_urls(self, html, url):
        selector = etree.HTML(html.encode('utf-8'))
        l_add_url = selector.xpath(r"//div[@data-role='ershoufang']/div[2]/a/@href")
        l_url = [url.join(r'https://bj.ke.com/chengjiao/', str(x).strip()) for x in l_add_url]
        print('parse l1 deal data of {} done, return {} urls'.format(url, len(l_url)))
        return l_url

    def parse_l2_data_2_urls(self, html, url):
        selector = etree.HTML(html.encode('utf-8'))
        l_next_level_urls = \
            [str(x) for x in selector.xpath(r"//div[@class='leftContent']//ul[@class='listContent']//li/a/@href")]
        #next_page
        next_page = None
        page_data = selector.xpath(r"//div[@class='page-box house-lst-page-box'][1]/@page-data")[0]
        mobj = re.match(r'{"totalPage":(\d+),"curPage":(\d+)}', page_data)
        curPage = int(mobj[2])
        totalPage = int(mobj[1])
        page_url = selector.xpath(r"//div[@class='page-box house-lst-page-box'][1]/@page-url")[0]
        if curPage+1 <= totalPage:
            next_page = urljoin('https://bj.ke.com/', str(page_url).format(page = curPage+1))
        print('parse l2 deal data of {} done, next_page is {}'.format(url, next_page))
        return next_page, l_next_level_urls

    def parse_l3_data_2_persist(self, html, url):
        r = DealInfo()
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
        return r

    def persist(self, list_r):
        try:
            session = DBSession()
            is_stop = None
            #is_stop = session.query(DealInfo).filter(DealInfo.code == list_r[0].code).one_or_none()
            for i in list_r:
                session.merge(i)
            session.commit()
            print('persist {} items of data, and is_stop = {}'.format(len(list_r), True if is_stop else False))

        except Exception as e:
            session.roolback()
            print(e)
        finally:
            session.close()
        return True if is_stop else False


if __name__ == '__main__':
    '''
    with open(os.path.join(proj_path, u'data/海淀/house_url/house_url_page1.html'), 'r') as f:
        r = HouseInfoHandler().parse_house_url(f.read(),
                                               os.path.join(proj_path, u'data/海淀/house_url/house_url_page1.html'))
    with open(os.path.join(proj_path, u'data/house_detail/101102870129.html'), 'r') as f:
        r = HouseInfoHandler().parse_house_info(f.read(),
                                                os.path.join(proj_path, u'data/house_detail/101102870129.html'))
        HouseInfoHandler().persis_house_info(r)
        print(r)
    with open(os.path.join(proj_path, u'data/朝阳/house_url/house_url_page1.html'), 'r') as f:
        r = RegionInfoHandler().parse_region(f.read(),
                                             os.path.join(proj_path, u'data/朝阳/house_url/house_url_page1.html'))
        RegionInfoHandler().persist_region(r)
        # print(r)
    '''
    #sale info test
    with open(os.path.join(proj_path, u'data/deal1_2.html'), 'r') as f:
        DealInfoHandler().parse_l2_data_2_urls(f.read(), 'test')
    with open(os.path.join(proj_path, u'data/deal_level_2_2.html'), 'r') as f:
        DealInfoHandler().parse_l3_data_2_persist(f.read(), 'test')
    #region info test
    with open(os.path.join(proj_path, u'data/region_1.html'), 'r') as f:
        RegionInfoHandler().parse_l1_data(f.read(), 'text')