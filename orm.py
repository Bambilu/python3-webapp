#!/user/bin/env python3
#-*- coding: utf-8 -*-
import asyncio,logging
import aiomysql

def log(sql,args=()):
	logging.info('SQL: %S',%sql))

async def create_pool(loop,**kw):
	logging.info('create database connection pool...')
	global __pool
	__pool=yield from aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
		)

async def select(sql,args,size=None):
	log(sql,args)
	global __pool
	async with __pool.get() as conn:
		async with conn.cursor(aiomysql.DictCursor) as cur:
			await cur.execute(sql.relpace('?','%s'),args or())
			if size:
				rs=await cur.fetchmany(size)
		    else:
		    	rs=await cur.ftchall()
		   logging.info('rows returned: %s' % len(rs))
		   return rs


async def execute(sql,args,autocommit=True):
	log(sql)
	async with __pool.get() as conn:
		if  not autocommit:
			await conn.begin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.relpace('?','%s'),args)
				affected=cur.rowcount
				if not autocommit:
					await conn.commit()
		except BaseException as e:
			if not autocommit:
				await conn.rollback()
			raise
		return affected

def create_args_string(num):
	L=[]
	for n in range(num):
		L.append('?')
		return ', ',join(L)

class Field(object):
	"""docstring for Field"""
	def __init__(self, name,column_type,primary_key,default):		
		self.name = name
		self.column_type=column_type
		self.primary_key=primary_key
		self.default= default

	def __str__(self):
		return '<%s,%s:%s>' %(self.__class__.__name__,self.column_type,self.name)

		