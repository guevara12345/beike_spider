#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import os
from datetime import date
import time
from lxml import etree


tb_house_info = ['code', 'total_price', 'unit_price', 'room',
                 'floor', 'build_area', 'huxing', 'house_area', 'orientations',
                 'buiding_texture', 'decoration', 'elevator_house_proportion', 'heating', 'is_elevator',
                 'property_right', 'building_type', 'xiaoqu', 'region', 'guapai_time',
                 'property_type', 'last_deal_time', 'house_usage', 'deal_year', 'property_ownership',
                 'mortgage', 'is_expire']
proj_path = os.path.abspath('..')


class RegionInfoHandler:
    chaoyang_far_region = ['北工大', '百子湾', '成寿寺', '常营', '朝阳门外',
                           'CBD', '朝青', '朝阳公园', '东坝', '大望路',
                           '东大桥', '大山子', '豆各庄', '定福庄', '方庄',
                           '垡头', '广渠门', '高碑店', '国展', '甘露园',
                           '管庄', '欢乐谷', '红庙', '华威桥', '酒仙桥',
                           '劲松', '建国门外', '农展馆', '潘家园', '石佛营',
                           '十里堡', '首都机场', '双井', '十里河', '十八里店',
                           '双桥', '三里屯', '四惠', '通州北苑', '团结湖',
                           '太阳宫', '甜水园', '望京', '西坝河', '燕莎', '中央别墅区', '朝阳其它']

    def parse_region(self, html, path):
        district = re.split(r'/', path)[6]
        soup = BeautifulSoup(html, 'html.parser')
        r = []
        l = soup.find('div', 'sub_sub_nav section_sub_sub_nav').find_all('a')
        for i in l:
            if i.string in RegionInfoHandler.chaoyang_far_region:
                is_far = 1
            else:
                is_far = 0
            r.append((i.string, district, is_far))
        print('parse {} for region info'.format(path, r))
        return r

    def persist_region(self, paras):
        db = db_helper.DbExeu()
        exists_sql = '''select * from tb_region_info where region=%s'''
        insert_sql = '''insert into tb_region_info(region,district,is_too_far) 
                             values (%s,%s,%s)'''
        for i in paras:
            if db.return_many_with_para(exists_sql, i[0])[1] == 0:
                db.trans(insert_sql, [i, ])
        print('persist {} region info done'.format(paras[0][1]))


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

    def parse_l1_deal_data(self, html):
        selector = etree.HTML(html.encode('utf-8'))
        l_next_level_urls = selector.xpath(r"//div[@class='leftContent']//ul[@class='listContent']//li/a/@href")
        l_next_pages = selector.xpath(r"//div[@class='page-box house-lst-page-box']//a")
        next_page_url = ''
        return temp, l_next_level_urls

    def parse_l2_deal_data(self, url):
        pass


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
    with open(os.path.join(proj_path, u'data/deal1.html'), 'r') as f:
        DealInfoHandler().parse_l1_deal_data(f.read())