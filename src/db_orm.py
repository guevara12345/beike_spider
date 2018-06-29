from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey, ForeignKeyConstraint
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.orm import sessionmaker, relationship
tb_house_info = ['code', 'total_price', 'unit_price', 'room',
                 'floor', 'build_area', 'huxing', 'house_area', 'orientations',
                 'buiding_texture', 'decoration', 'elevator_house_proportion', 'heating', 'is_elevator_exist',
                 'property_right', 'building_type', 'xiaoqu', 'region', 'guapai_time',
                 'property_type', 'last_deal_time', 'house_usage', 'deal_year', 'property_ownership',
                 'mortgage', 'is_expire']

engine = create_engine('mysql+pymysql://user:P@ssw0rd@localhost/db_beike?charset=utf8')
Base = declarative_base()


class SaleInfo(Base):
    __tablename__ = 'tb_sale_info'
    __table_args__ = (
        ForeignKeyConstraint(
            ['region', 'district'],
            ['tb_region_info.region', 'tb_region_info.district'],
        ),
    )

    code = Column(String(100), primary_key=True)
    total_price = Column(String(100))
    unit_price = Column(String(100))
    room = Column(String(100))
    floor = Column(String(100))

    build_area = Column(String(100))
    huxing = Column(String(100))
    house_area = Column(String(100))
    orientations = Column(String(100))
    buiding_texture = Column(String(100))

    decoration = Column(String(100))
    elevator_house_proportion = Column(String(100))
    heating = Column(String(100))
    is_elevator_exist = Column(String(100))
    property_right = Column(String(100))

    building_type = Column(String(100))
    xiaoqu = Column(String(100))
    region = Column(String(100))
    guapai_time = Column(String(100))
    property_type = Column(String(100))

    last_deal_time = Column(String(100))
    house_usage = Column(String(100))
    deal_year = Column(String(100))
    guapai_time = Column(String(100))
    property_ownership = Column(String(100))

    mortgage = Column(String(100))
    district = Column(String(100))
    is_expire = Column(Integer)



class RegionInfo(Base):
    __tablename__ = 'tb_region_info'

    region = Column(String(100), primary_key=True)
    district = Column(String(100), primary_key=True)
    is_too_far = Column(Integer)


class DealInfo(Base):
    __tablename__ = 'tb_deal_info'
    __table_args__ = (
        ForeignKeyConstraint(
            ['region', 'district'],
            ['tb_region_info.region', 'tb_region_info.district'],
        ),
    )
    total_price = Column(String(100))
    unit_price = Column(String(100))
    # 房屋户型
    room = Column(String(100))
    # 所在楼层
    floor = Column(String(100))
    # 建筑面积
    build_area = Column(String(100))
    # 户型结构
    huxing = Column(String(100))
    # 套内面积
    house_area = Column(String(100))
    # 建筑类型
    building_type = Column(String(100))
    # 房屋朝向
    orientations = Column(String(100))
    # 建成年代
    buiding_time = Column(String(100))
    # 装修情况
    decoration = Column(String(100))
    # 建筑结构
    buiding_texture = Column(String(100))
    # 供暖方式
    heating = Column(String(100))
    # 梯户比例
    elevator_house_proportion = Column(String(100))
    # 产权年限
    property_time = Column(String(100))
    # 配备电梯
    is_elevator = Column(String(100))

    # 链家编号
    code = Column(String(100), primary_key=True)
    # 交易权属
    property_type = Column(String(100))
    # 挂牌时间
    guapai_time = Column(String(100))
    # 房屋用途
    house_usage = Column(String(100))
    # 房屋年限
    deal_year = Column(String(100))
    # 房权所属
    property_ownership = Column(String(100))

    district = Column(String(100))
    region = Column(String(100))
    xiaoqu = Column(String(100))
    deal_time = Column(String(100))
    url = Column(String(100))
    is_expire = Column(Integer)


Base.metadata.create_all(engine)
DBSession = sessionmaker(bind=engine)