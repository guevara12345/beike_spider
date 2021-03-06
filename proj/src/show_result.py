from db_orm import DealInfo, RegionInfo, SaleInfo, DBSession
from sqlalchemy import and_, or_
import csv
import os
import time

import config


session = DBSession()


def show_sale_info():
    query = session.query(SaleInfo.xiaoqu, SaleInfo.total_price, SaleInfo.unit_price, SaleInfo.district,SaleInfo.region,
                          SaleInfo.build_area, SaleInfo.house_area, SaleInfo.room, SaleInfo.orientations,
                          SaleInfo.guapai_time, SaleInfo.url, RegionInfo). \
        filter(and_(SaleInfo.region == RegionInfo.region,
                    SaleInfo.district == RegionInfo.district,
                    SaleInfo.house_usage == '普通住宅',
                    RegionInfo.is_too_far == 0,
                    SaleInfo.total_price >= '650',
                    SaleInfo.total_price <= '850',
                    SaleInfo.is_expire == 0)). \
        filter(or_(SaleInfo.district == '朝阳', SaleInfo.district == '海淀', SaleInfo.district == '丰台')).\
        order_by(SaleInfo.district).order_by(SaleInfo.region).order_by(SaleInfo.xiaoqu)
    l_r = query.all()
    l_t_r = [(x.xiaoqu, x.total_price, x.unit_price, x.district, x.region, x.build_area, x.house_area, x.room, x.orientations,
              x.guapai_time, x.url) for x in l_r]
    with open(os.path.join(config.PROJ_PATH, 'result/house_info_{}.csv'.format(time.strftime("%Y-%b-%d_%H%M%S"))),
              'w', encoding='utf_8_sig', newline='') as f:
        f_csv = csv.writer(f, dialect='excel')
        f_csv.writerow(
            ['xiaoqu', 'total_price', 'unit_price','district', 'region', 'build_area', 'house_area', 'room', 'orientations',
             'guapai_time', 'url'])
        f_csv.writerows(l_t_r)

def test():
    session.query(SaleInfo).filter().update({SaleInfo.is_expire: 0})
    session.commit()


if __name__ == '__main__':
    show_sale_info()
    #test()