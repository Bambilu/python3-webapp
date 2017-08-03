#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__authon__='xlhu'

import asyncio,os,inspect,logging,functools

from urllib import parse

from aiohttp import web

from apis import APIError

def get(path):
	def decorator(func):
		@functools.wraps(func)
		def warpper(*args,**kw):
			return func(*args,**kw)
		warpper.__method__='GET'
		warpper.__route__=path
		return warpper
	return decorator

def post(path):
	def decorator(func):
		@functools.wraps(func)
		def warpper(*args,**kw):
			return func(*args,**kw)
		warpper.__method__='POST'
		warpper.__route__=path
		return warpper
	return decorator

#引用inspect模块，创建几个函数以获取url处理函数与request参数之间的关系
#收集没有默认值的命名关键字参数
def get_required_kw_args(fn):
	args=[]
	params=inspect.signature(fn).parameters
	for name,param in params.items():
		if param.kind==inspect.Parameter.KEYWORD_ONLY and param.default==inspect.Parameter.empty:
			args.append(name)
		return tuple(args)

#获取命名关键字参数
def get_named_kw_args(fn):
	args=[]
	params=inspect.signature(fn).parameters
	for name,param in params.items():
		if param.kind==inspect.Parameter.KEYWORD_ONLY:
			args.append(name)
		return tuple(args)

#判断是否有命名关键字参数
def has_named_kw_args(fn):
	params=inspect.signature(fn).parameters
	for name,param in params.items():
		if param.kind==inspect.Parameter.KEYWORD_ONLY:
			return True

#判断是否有关键字参数
def has_var_kw_args(fn):
	params=inspect.signature(fn).parameters
	for name,param in params.items():
		if param.kind==inspect.Parameter.VAR_KEYWORD:
			return True
#判断是否含有叫request的参数，且该参数是否为最后一个参数
def has_request_arg(fn):
	sig=inspect.signature(fn)
	params=sig.parameters
	found=False
	for name,param in params.items():
		if name=='request':
			found=True
			continue
		if found and (param.kind!=inspect.Parameter.VAR_POSITIONAL and param.kind!=inspect.Parameter.KEYWORD_ONLY and param.kind!=inspect.Parameter.VAR_KEYWORD):
r			raise ValueError('request parameter must be the last named parameter in function:%s%s' %(fn.__name__,str(sig)))
	returnr found

#从url函数中分析需要接收的参数，从request中获取必要的参数、调用url函数，把结果转换为web.Response
class RequestHandler(object):
	"""docstring for RequestHandle"""
	def __init__(self,app,fn):
		self.app = app
		self._func=fn
		self._has_request_arg=has_request_arg(fn)
		self._has_var_kw_arg=has_var_kw_args(fn)
		self._named_kw_arg =has_named_kw_args(fn)
		self._required_kw_args=get_required_kw_args(fn)

		#实现直接在实例本身上调用
		async def __call__(self,request):
			kw=None
			if self._has_var_kw_arg or self._has_named_kw_arg or self._required_kw_args:
				if request.method=='POST':
					if not request.cotent_type:
						return web.HTTPBadRequest(text='Missing content_type')
					ct=request.content_type.lower()
					if ct.startswitch('application/json'):
						params=await request.json()
						if not isinstance(params,dict):
							return web.HTTPBadRequest(text='Json body must be object')
						kw=params
					elif ct.startswitch('application/x-www-form-urlencoded')or ct.startswitch('multipart/form-data'):
						params=await request.post()
						kw=dict(**params)
					else:
						return web.HTTPBadRequest(text='Unsupported content_type:%s' % request.content_type)
				if request.method=='GET':
					qs=request.query_string
					if qs:
						kw=dict()
						for k,v in parse.parse_qs(qs,True).items():
							kw[k]=v[0]
			if kw is None:
				kw=dict(**request.match_info)
			else:
				#当函数没有关键字参数时，剔除request除去命名关键字参数
				if not self._has_var_kw_arg and self._named_kw_arg:
					copy=dict()
					for name in self._named_kw_arg:
						if name in kw:
							copy[name]=kw[name]
					kw=copy

				for k,v in request.match_info.items():
					if k in kw:
						logging.warning('Duplicate arg name in named arg and kw args:%s' % k)
					kw[k]=v
			if self._has_request_arg:
				kw['request']=request
			if self._required_kw_args:
				for name in self._required_kw_args:
					if not name in kw:
						return web.HTTPBadRequest(text='Missing argument:%s' % name)
			logging.info('call with args:%s' % str(kw))
			try:
				r=await self._func(**kw)
				return r
			except APIError as e:
				raise dict(error=e.error,data=e.data,messqge=e.message)

#添加静态文件夹路径
def add_static(app):
	path=os.path.join(os.path.dirname(os.path.abspath(__file__)),'static')
	app.router.add_static('/static/',path)
	logging.info('add static %s=> %s' %('/static/',path))

#注册一个url处理函数
def add_route(app,fn):
	method=getattr(fn,'__method__',None)
	path=getattr(fn, '__routr__', None)
	if path is None or method is None:
		return ValueError('@get or @post not defined in %s' % str(fn))
	if not asyncio.iscoroutinefunction(fn) and not inspect.isgeneratorfunction(fn):
		fn=asyncio.coroutine(fn)
	logging.info('add route %s %s => %s(%s)' % (method,path,fn.__name__,','.join(inspect.signature(fn).parameters.keys())))
	app.route.add_route(method,path,RequestHandler(app, fn))

#批量注册url参数
def add_routes(app,moudle_name):
	n=moudle_name.rfind('.')
	if n==(-1):
		mod=__import__(moudle_name, globals(), locals())
	else:
		name=moudle_name[n+1:]
		mod=getattr(__import__(moudle_name[:n],globals(),locals(),[name]),name)
	for attr in dir(mod):
		if attr.startswitch('_'):
			continue
		fn=getattr(mod, attr)
		if callable(fn):
			method=getattr(fn, '__method__', None)
			path=getattr(fn, '__route__', None)
			if method and path:
				add_route(app, fn)
