from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.orm import sessionmaker, relationship
tb_house_info = ['code', 'total_price', 'unit_price', 'room',
                 'floor', 'build_area', 'huxing', 'house_area', 'orientations',
                 'buiding_texture', 'decoration', 'elevator_house_proportion', 'heating', 'is_elevator_exist',
                 'property_right', 'building_type', 'xiaoqu', 'region', 'guapai_time',
                 'property_type', 'last_deal_time', 'house_usage', 'deal_year', 'property_ownership',
                 'mortgage', 'is_expire']

engine = create_engine('mysql+mysqldb://user:P@ssw0rd@localhost/db_beike?charset=utf8')
Base = declarative_base()
Base.metadata.create_all(engine)


class Orm:
    def get_session(self):
        Session = sessionmaker(bind=engine)
        session = Session()
        return session


class SaleInfo(Base):
    __tablename__ = 'tb_sale_info'

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
    region = Column(String(100), ForeignKey('tb_region_info.region'))
    guapai_time = Column(String(100))
    property_type = Column(String(100))

    last_deal_time = Column(String(100))
    house_usage = Column(String(100))
    deal_year = Column(String(100))
    guapai_time = Column(String(100))
    property_ownership = Column(String(100))

    mortgage = Column(String(100))
    is_expire = Column(String(100))


class RegionInfo(Base):
    __tablename__ = 'tb_region_info'

    id = Column(Integer, primary_key=True, autoincrement=True)
    region = relationship('tb_sale_info')
    district = Column(String(100))
    is_too_far = Column(Integer)


class DealInfo(Base):
