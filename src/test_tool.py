import src.db_helper
import csv
import os
import time

coloum = ['code','total_price','unit_price','room',
         'floor','build_area','huxing','house_area','orientations',
         'buiding_texture','decoration','elevator_house_proportion','heating','is_elevator',
         'property_right','building_type','xiaoqu','region','guapai_time',
         'property_type','last_deal_time','house_usage','deal_year','property_ownership',
         'mortgage','is_expire']
proj_path = os.path.abspath('..')

def select():
    db_hanlder = src.db_helper.DbExeu()
    sql = '''
        SELECT t.xiaoqu, CONCAT(t.total_price, '万'), t.unit_price, t.region, t.build_area
            , t.house_area, t.room, t.orientations, t.guapai_time
            , CONCAT('https://bj.lianjia.com/ershoufang/', t.code, '.html')
        FROM tb_house_info t, tb_region_info s
        WHERE t.region = s.region
            AND house_usage = '普通住宅'
            AND s.is_too_far = 0
            AND t.total_price>='650'
            AND t.total_price<='800'
            AND t.is_expire='0'
            AND (s.district ='朝阳' OR s.district ='海淀')
        ORDER BY region, xiaoqu
        '''
    r = db_hanlder.return_many_without_para(sql)
    time_str = time.strftime("%Y-%b-%d %H%M%S")
    with open(os.path.join(proj_path, 'result/house_info_{}.csv'.format(time_str)), 'w') as f:
        i=list(r[0])
        f_csv = csv.writer(f)
        f_csv.writerow(['xiaoqu','total_price','unit_price','region','build_area','house_area', 'room','orientations','guapai_time','href'])
        f_csv.writerows(i)
    sql = '''
        SELECT s.region, s.xiaoqu, t.price_change, t.total_price, t.datetime
            , CONCAT('https://bj.lianjia.com/ershoufang/', t.code, '.html')
        FROM tb_price_change t, tb_house_info s
        WHERE t.code = s.code
        ORDER BY s.region, s.xiaoqu, t.code, t.datetime
    '''
    r = db_hanlder.return_many_without_para(sql)
    with open(os.path.join(proj_path, 'result/price_change_{}.csv'.format(time_str)), 'w') as f:
        i=list(r[0])
        f_csv = csv.writer(f)
        f_csv.writerow(['region','xiaoqu','price_change','total_price','datetime','code'])
        f_csv.writerows(i)

    sql = '''
            SELECT t.xiaoqu, count(t.code)
            FROM tb_house_info t, tb_region_info s
            WHERE t.region = s.region
                AND house_usage = '普通住宅'
                AND s.is_too_far = 0
                AND t.total_price>='650'
                AND t.total_price<='850'
                AND t.is_expire='0'
                AND (s.district ='朝阳' OR s.district ='海淀')
            group by t.xiaoqu
            order by count(t.code)
            '''
    r = db_hanlder.return_many_without_para(sql)
    for x in r[0]:
        print(x)

def select_test():
    db_hanlder = src.db_helper.DbExeu()
    sql = '''select * from tb_region_info'''
    r = db_hanlder.return_many_without_para(sql)
    print(r)
    sql = '''
        SELECT t.code, t.total_price, t.price_change, t.datetime, s.xiaoqu
            , s.region
        FROM tb_price_change t, tb_house_info s
        WHERE t.code = s.code
    '''
    r = db_hanlder.return_many_without_para(sql)
    for x in r[0]:
        print(x)


if __name__=='__main__':
    select()
