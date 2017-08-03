#!/user/bin/env python3
#@-*- coding:utf-8 -*-

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
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
		)


async def destory_pool():
	global __pool
	if __pool is not None:
		__pool.close()
		await __pool.wait_closed()
		
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

	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):

	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
	def __init__(self, name=None,default=False):
		super().__init__(name,'boolean',False,default)
		
	
class IntegerField(Field):
		"""docstring for IntegerField"""
		def __init__(self, name=None,primary_key=False,default=0):
			super().__init__(name,'bigint',primary_key,default)


class FloatField(Field):
	"""docstring for FloatField"Field"""
	def __init__(self, name=None,primary_key=False,default=0.0):
		super().__init__(name,'real',primary_key,default)


class TextField(Field):
	"""docstring for TextField"""
	def __init__(self, name=None,default=None):
		super().__init__(name,'text',False,default)
		
class ModelMetaclass(type):
		def __new__(cls, name,bases,attrs):
			'''
			创建模型与表映射的基类
			：name:类名
			：bases：父类
			：attrs：类的属性列表
			：return：模型元类
			'''
			#排除Model类本身
			if name=='Model':
				return type.__new__(cls,name,bases,attrs)
			#获取表名，如果没有表名则将类名作为表名
			tablenName=attrs.get('__table__',None)or name
			logging.info('模型：%s (表名：%s)' %(name,tablenName))
			#获取所有的类属性和主键名
			mappings=dict()																#存储属性名和字段信息的映射关系
			fields=[]																	#存所有非主键的属性
			primaryKey=None															#存主键属性
			for k,v in attrs.items():													#遍历attrs（类的所有属性），k属性名，v对应字段
				if isinstance(v, Field):												#如果v是自定义的字段类型
					logging.info('映射关系：%s==>%s' %(k,v))
					mappings[k]=v 														#存储映射关系
					if v.primary_key:													#如该属性为主键
						if primaryKey:													#如primaryKey不为None，则主键已存在，主键重复
							raise RuntimeError('主键重复：在%s中的%s' % (name,k))
						primaryKey=k
					else:																#如不为主键 ，存到fields列表
						fields.append(k)

			if not primaryKey:															#如遍历结束后，主键为None，则主键未定义
					raise RuntimeError('主键未定义：%s' % name)
			for k in mappings.keys():													#清空attrs
				attrs.pop(k)
			# 将fields中属性名以‘属性名’的方式装饰
			escaped_fields=list(map(lambda f:'`%s`' % f,fields))
			#重新设置attrs
			attrs['__mappings__']=mappings										#保存属性和字段信息的映射关系
			attrs['__table__'] = tablenName
			attrs['__primary_key__']=primaryKey
			attrs['__fields__']=fields

			#构造默认的SELECT INSERT UPDATE DELETE语句
			attrs['__select__']='select `%s`,%s from `%s`' %(primaryKey,', '.join(escaped_fields),tablenName)
			attrs['__insert__']='insert into `%s` (%s,`%s`) values(%s)'% (tablenName,', '.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
			attrs['__update__']='update `%s` set %s where `%s`=?' %(tablenName,', '.join(map(lambda f:'`%s`=?' %(mappings.get(f).name or f),fields)),primaryKey)
			attrs['__delete__']='delete from `%s` where `%s`=?' % (tablenName,primaryKey)

			return type.__new__(cls,name,bases,attrs)


class Model(dict,metaclass=ModelMetaclass):
	def __init__(self,**kw):
		super(Model,self).__init__(**kw)

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' 对象没有属性'%s' " % key)

	def __setattr__(self,key,value):
		self[key]=value

	def getValue(self,Key):
		return getattr(self,Key,None)


	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:                           # 如果没有找到value
			field = self.__mappings__[key]           # 从mappings映射集合中找
			value = field.default() if callable(field.default) else field.default
			logging.debug('使用默认值 %s:%s' % (key, str(value)))
			setattr(self, key, value)
		return value


	#类方法第一个参数是cls（当前准备创建的类的对象），实例方法第一个参数是self
	@classmethod
	async def findAll(cls,where=None,args=None,**kw):
		'''
		通过where查找多条记录
		:where:查询调价
		：args：sql参数
		：kw：查询条件列表
		：return：多条记录集合
		'''
		sql=[cls.__select__]
		if where:
			sql.append('where')#添加where关键字
			sql.append(where)#拼接where条件

		if args is None:
			args=[]

		orderBy=kw.get('OrderBy',None)
		if orderBy:
			sql.append('orderBy')
			sql.append(orderBy)

		limit =kw.get('limit',None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit,int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple)and len(limit)==2:
				sql.append('?,?')
				args.extend(limit)
			else:
				raise ValueError('limit参数无效：%s' % str(limit))
		rs=await select(''.join(sql), args)
		return [cls(**r) for r in rs]

	@classmethod
	async def findNumbers(cls,selectField,where=None,args=None):
		'''
		查询某个字段的数量
		'''
		sql=['select count(%s) _num_ from `%s`' %(StringField,cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs=await select(''.join(sql), args,1)
		return rs[0]['_num_']


	@classmethod
	async def findById(cls,pk):
		rs=await select('%s where `%s`=?' % (cls.__select__,cls.__primary_key__), [pk],1)
		if len(rs)==0:
			return None
		return cls(**rs[0])


	@classmethod
	async def findByColumn(cls,f,cl):
		fi=None
		for field in cls.__fields__:
			if f==field:
				fi=field
				break
		if fi is not None:
			raise AttributeError('在%s中没找到该字段：' %cls.__table__)
		rs=await select('%s where `%s`=?'%(cls.__select__,fi),[cl],1)
		if len(rs)==0:
			return None
		return cls(**rs[0])

	async def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = await execute(self.__insert__, args)
		if rows != 1:
			logging.warn('failed to insert record: affected rows: %s' % rows)

	async def update(self):
		args=list(map(self.getValue(self.__fields__)))
		args.append(self.getValue(self.__primary_key__))
		rows=await execute(self.__update__, args)
		if  rows!=1:
			logging.warning('更新记录失败：受影响行数：%s' % rows)

	async def delete(self):
		args=[self.getValue(self.__primary_key__)]
		rows=await execute(self.__delete__, args)
		if  rows!=1:
			logging.warning('删除记录失败：受影响行数：%s' % rows)