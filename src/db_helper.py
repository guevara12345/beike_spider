#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql


class DbExeu:

    def get_start(self, command):
        try:
            con = pymysql.connect(host="localhost", user="user", password="P@ssw0rd", db="db_lianjia", charset='utf8')
            cur = con.cursor()
            if not command == None:
                cur.execute(command)
        except Exception as e:
            print(e)
            raise e
        finally:
            cur.close()
            con.close()

    def return_many_with_para(self, sql, para):
        #print('sql={}\npara={}'.format(sql, para))
        try:
            con = pymysql.connect(host="localhost", user="user", password="P@ssw0rd", db="db_lianjia", charset='utf8')
            cur = con.cursor()
            num = cur.execute(sql,para)
            t = cur.fetchall()
            return (t, num)
        except Exception as e:
            print(e)
            raise e
        finally:
            cur.close()
            con.close()

    def return_many_without_para(self, sql):
        #print('sql={}\npara={}'.format(sql))
        try:
            con = pymysql.connect(host="localhost", user="user", password="P@ssw0rd", db="db_lianjia", charset='utf8')
            cur = con.cursor()
            num = cur.execute(sql)
            t = cur.fetchall()
            return (t, num)
        except Exception as e:
            print(e)
            raise e
        finally:
            cur.close()
            con.close()

    def trans(self, sql, paras):
        #print('sql={}\npara={}'.format(sql, para))
        try:
            con = pymysql.connect(host="localhost", user="user", password="P@ssw0rd", db="db_lianjia", charset='utf8')
            cur = con.cursor()
            cur.executemany(sql, paras)
            con.commit()
        except Exception as e:
            con.rollback()
            print(paras)
            print('Error: {}'.format(e))
            raise e
        finally:
            cur.close()
            con.close()


#creat the datebase and table
if __name__=='__main__':
    obj=DbExeu()
    sql1 = '''
        CREATE TABLE IF NOT EXISTS tb_house_info (
            mortgage varchar(100),
            code varchar(100) PRIMARY KEY UNIQUE,
            total_price varchar(100),
            unit_price varchar(100),
            room varchar(100),
            floor varchar(100),
            build_area varchar(100),
            huxing varchar(100),
            house_area varchar(100),
            orientations varchar(100),
            buiding_texture varchar(100),
            decoration varchar(100),
            elevator_house_proportion varchar(100),
            heating varchar(100),
            is_elevator varchar(100),
            property_right varchar(100),
            building_type varchar(100),
            xiaoqu varchar(100),
            region varchar(100),
            guapai_time varchar(100),
            property_type varchar(100),
            last_deal_time varchar(100),
            house_usage varchar(100),
            deal_year varchar(100),
            property_ownership varchar(100),
            is_expire varchar(100))
            default charset=utf8;
            '''
    sql2 = '''
        CREATE TABLE IF NOT EXISTS tb_region_info ( 
            region varchar(100) PRIMARY KEY UNIQUE,
            district varchar(100),
            is_too_far int)
            default charset=utf8; 
    '''
    sql3 = '''
        CREATE TABLE IF NOT EXISTS tb_price_change (
            timestamp varchar(100) PRIMARY KEY UNIQUE,
            code varchar(100),
            total_price varchar(100),
            price_change varchar(100),
            datetime varchar(100))
            default charset=utf8; 
    '''
    obj.get_start(sql1)
    obj.get_start(sql2)
    obj.get_start(sql3)
