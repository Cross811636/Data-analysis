#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 19:05:13 2021
用python读csv写入mysql
@author: qingzhikeji
"""


import pandas as pd
import pymysql
import time, datetime

def get_data(file_name):
    # 用pandas读取csv
    # data = pd.read_csv(file_name,engine='python',encoding='gbk')
    data = pd.read_csv(file_name,engine='python')
    print (data.head(5)) #打印前5行
    
    # 数据库连接
    conn = pymysql.connect(
		user="用户名",
		port=端口,
		passwd="密码",
		db="数据库名",
		host="127.0.0.1",
		charset = 'utf8'
    )
        
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()

    # 数据过滤，替换 nan 值为 None
    data = data.astype(object).where(pd.notnull(data), None) 
	
    for ID,CREATE_DATE,CREATE_USER_ACCOUNT,UPDATE_DATE,UPDATE_USER_ACCOUNT,TASK_ID,USER_ACCOUNT,USER_PERSON_TYPE_ID,USER_NAME,COOPERATIVE_MEMBER in zip(data['ID'],data['CREATE_DATE'],data['CREATE_USER_ACCOUNT'],data['UPDATE_DATE'],data['UPDATE_USER_ACCOUNT'],data['TASK_ID'],data['USER_ACCOUNT'],data['USER_PERSON_TYPE_ID'],data['USER_NAME'],data['COOPERATIVE_MEMBER']):

        CREATE_DATE = format_date(CREATE_DATE) # 这里由于对日期有特殊需求，自己处理了一下，代码就不贴了，如无需要可略过。
        
        dataList = [ID,CREATE_DATE,CREATE_USER_ACCOUNT,UPDATE_DATE,UPDATE_USER_ACCOUNT,TASK_ID,USER_ACCOUNT,USER_PERSON_TYPE_ID,USER_NAME,COOPERATIVE_MEMBER]

        print (dataList) # 插入的值
        
        try:
            insertsql = "INSERT INTO sw_task_member_asign(ID,CREATE_DATE,CREATE_USER_ACCOUNT,UPDATE_DATE,UPDATE_USER_ACCOUNT,TASK_ID,USER_ACCOUNT,USER_PERSON_TYPE_ID,USER_NAME,COOPERATIVE_MEMBER) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insertsql,dataList)
            conn.commit()
        except Exception as e:
            print ("Exception")
            print (e)
            conn.rollback()
            
    cursor.close()
    # 关闭数据库连接
    conn.close()

def main():
    # 读取数据
    get_data('xxx.csv')


if __name__ == '__main__':
    main()
