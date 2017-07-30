#!/user/bin/env python3
@-*- coding:utf-8 -*-

__author__='xlhu'

import aiomysql
import asyncio,logging

def log(sql,args=()):
	logging.info('SQL:%s' % sql)

'''
创建数据库连接池
：loop：事件循环处理程序
：kw：数据库配置参数集合
：return：无
'''
async def create_pool(loop,**kw):
	logging.info('create database connection pool...')
	global __pool
	__pool=await aiomysql.create_pool(
		host= kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password = kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw,get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
		)
'''
数据库查询
：sql：sql语句
：args：sql参数
：size：要查询的数量
：return：查询结果
'''
async def select(sql,args,size=None):
	log(sql,args)
	global __pool
	async with __pool.get() as conn:
		#创建一个结果为字典的游标
		async with conn.cursor(aiomysql.DictCursor)as cur:
			await cur.execute(sql.replace('?','%s'),args or())
			if size:
				rs=await cur.fetchmany(size)
			else:
				rs =await cur.fetchall()
		logging.info('rows returned: %s' % len(rs))
		return rs

'''
数据库 Insert Update Delete 操作函数
：sql：sql语句
：args：sql参数
：autocommit：自动提交事务

'''
async def execute(sql,args,autocommit=True):
	log(sql,args)
	async with __pool.get() as conn:
		if not autocommit:
			await conn.begin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?','%s'),args)
				#操作的记录数
				affected=cur.rowcount
			if not autocommit:
				await conn.commit()
		except BaseException as e:
			if not autocommit:
				await conn.rollback()
			raise e
		return affected

'''
计算需要拼接多少个占位符
'''
	def create_args_string(num):
		L=[]
		for n in range(num):
			L.append('?')
		return ', '.join(L)


class Field(object):
	def __init__(self, name,column_type,primary_key,default):
		self.name = name
		self.column_type= column_type
		self.primary_key = primary_key
		self.default=default

	def __str__(self):
		return '<%s,%s:%s>' %(self.__class__.name,self.column_type.self.name)


class StringField(Field):
	"""docstring for StringField"""
	def __init__(self, arg):
		super StringField, self).__init__()
		self.arg = arg
		