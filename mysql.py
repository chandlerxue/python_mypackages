#-*-coding:utf-8-*-  
#mysql.py
'''
#-----------------------------------------------------------
# @author     道心空
# @qq		  342701293
# @date       2013-08-17
# @decscript  mysql 操作类  CURD 封装
#	
#-----------------------------------------------------------
'''
#导入 MySQLLdb 		库
import MySQLdb;					


class Mysql(object):
	# 私有变量
	__user = '';
	__db = '';
	__passwd = '';
	__host = '';
	__charset = '';
	__conn ='';

	"""docstring for Mysql"""
	def __init__(self, data):

		super(Mysql, self).__init__()
		self.__user    = db['user'];
		self.__db      = db['db'];
		self.__passwd  = db['passwd']; 
		self.__host    = db['host'];
		self.__charset = db['charset'];


	def conn(self):
		try:
			self.__conn =  MySQLdb.connect(user=self.__user, db=self.__db, passwd=self.__passwd, host=self.__host , charset=self.__charset) ; 
			self.__conn = self.__conn.cursor( cursorclass=MySQLdb.cursors.DictCursor);
			#MySQLdb.cursors.Cursor， 默认值，执行SQL语句返回List，每行数据为tuple
			#MySQLdb.cursors.DictCursor， 执行SQL语句返回List，每行数据为Dict

		except MySQLdb.Error,e:
			return "Mysql Error %d: %s" % (e.args[0], e.args[1]);

	"""
	* -------------------------------------------------------------
	* 执行查询 返回数据集
	* -------------------------------------------------------------
	* @access public
	* -------------------------------------------------------------
	* @param list column 		[column1, column2, column3 ...]
	* -------------------------------------------------------------
	* -------------------------------------------------------------
	"""	
	def column( self,column ):
		result = '';
		for item in column:
			if result != '':
				result = result + ',' + item; 
			else:
				result = item;

		return result;

	def where( self,where ):
		if where != '':
			where = ' WHERE ' + where;
		else:
			where = '';
		return where;

	def order( self,order ):
		if order != '':
			order = ' ORDER BY ' + order;
		else:
			order = '';
		return order;

	def limit( self,limit ):
		if limit != '':
			limit = ' LIMIT ' + limit;
		else:
			limit = '';
		return limit;
			
	"""
	* -------------------------------------------------------------
	* 执行查询 返回数据集
	* -------------------------------------------------------------
	* @access public
	* -------------------------------------------------------------
	* @param str 	table 
	* @param list 	column		[column1, column2,column3 ...]
	* @param str 	where		
	* @prarm str 	order		
    * @param str 	limit
	* @example
	*
	"""
	def ___query( self, table, column='*', where='' , order='', limit = '' ):
		column = self.column( column );
		where = self.where(where);
		order = self.order(order);
		limit = self.limit(limit);
		sql = 'SELECT ' + column + ' FROM ' + table + '  ' + where  + '  ' + order + ' '  + limit;

		try:
			return self.__conn.execute (sql);
			#return 'there has %s rows record' % count;							#返回受影响记录行数
			#return sql;

		except MySQLdb.Error,e:
			return "Mysql Error %d: %s" % (e.args[0], e.args[1]);
		

	"""
	* -------------------------------------------------------------
	* 返回数据集受影响纪录数
	* -------------------------------------------------------------
	* @access public
	* -------------------------------------------------------------
	* @param str 	table 
	* @param list 	column		[column1, column2,column3 ...]
	* @param str 	where		
	* @prarm str 	order		
    * @param str 	limit
	* @example
	*--------------------------------------------------------------
	"""
	def count(self, table, column='*', where='' , order='', limit = ''):
		count = self.___query(table, column, where, order, limit);
		return count;


	"""
	* -------------------------------------------------------------
	* 返回数据集受影响的一行纪录
	* -------------------------------------------------------------
	* @access public
	* -------------------------------------------------------------
	* @param str 	table 
	* @param list 	column		[column1, column2,column3 ...]
	* @param str 	where		
	* @prarm str 	order		
    * @param str 	limit
	* @example
	*--------------------------------------------------------------
	"""
	def singleOne(self, table, column='*', where='' , order='', limit = '1'):
		count = self.___query(table, column, where, order, limit);
		results = self.__conn.fetchone();
		#fetchall()
		#fetchone()
		#fetchmany(5)
		return result;


	"""
	* -------------------------------------------------------------
	* 返回数据集受影响的多行纪录集
	* -------------------------------------------------------------
	* @access public
	* -------------------------------------------------------------
	* @param str 	table 
	* @param list 	column		[column1, column2,column3 ...]
	* @param str 	where		
	* @prarm str 	order		
    * @param str 	limit
	* @example
	*--------------------------------------------------------------
	"""
	def mutilRecord(self, table, column='*', where='' , order='', limit = ''):
		count = self.___query(table, column, where, order, limit);
		results = self.__conn.fetchall();
		#fetchall()
		#fetchone()
		#fetchmany(5)
		return results;		


	def insertColumn(self, column):
		local_columns = '';
		for item in column:
			if local_columns  != '':
				local_columns  =  local_columns + ',' + (  '%s' % item ); 
				local_value    =  local_value   + ",'" + column[item] + "'";
			else:
				local_columns = (  '%s' % item );
				local_value =  "'" + column[item] + "'";


		results = ' ( ' + local_columns + ' ) VALUES ( ' + local_value +' ) ';

		return results;		


	"""
	* ----------------------------------------------------------------------
	* 添加
	* ----------------------------------------------------------------------
	* @access public
	* ----------------------------------------------------------------------
	* @param str 	table 
	* @param list 	column		{'column1':value, 'column2':vaue2 ...}		
	* ----------------------------------------------------------------------
	"""
	def add( self, table, column ):
		column = self.insertColumn(column);
		sql = 'INSERT INTO ' + table + column ;

		try:
			status = self.__conn.execute (sql);					#stauts=1 ，添加数据成功
			return status;
			#self.__conn.commit();
		except MySQLdb.Error,e:
			return "Mysql Error %d: %s" % (e.args[0], e.args[1]);		
		


	"""
	* ------------------------------------------
	* 删除
	* ------------------------------------------
	* @access public
	* ------------------------------------------
	* @param str table
	* @param str where
	"""
	def delete( self, table, where ):
		where = self.where(where);
		sql = 'DELETE FROM ' + table + where ;

		#return sql;
		try:
			status = self.__conn.execute (sql);					#stauts=1 ，删除数据成功
			return status;
			#self.__conn.commit();


		except MySQLdb.Error,e:
			return "Mysql Error %d: %s" % (e.args[0], e.args[1]);		
		

	def updateColumn( self, column ):
		results = '';
		for item in column:
			if results  != '':
				results  =  results + ', ' + (  '%s' % item )  + ' = "' + column[item] + '" ' ; 
				#local_value    =  local_value   + ",'" + column[item] + "'";
			else:
				results  =  results + ' ' + (  '%s' % item )  + ' = "' + column[item] + '" ' ; 
				#local_value =  "'" + column[item] + "'";


		##results = ' ( ' + local_columns + ' ) VALUES ( ' + local_value +' ) ';

		return results;		

	'''
	* ------------------------------------------
	* update
	* ------------------------------------------
	* @access public
	* ------------------------------------------
	* @param
	'''
	def update( self, table, column , where ='' ):
		column = self.updateColumn(column);
		where = self.where(where);
		sql = 'UPDATE ' + table + ' SET '  + column + ' ' + where;

		return sql;
		try:
			status = self.__conn.execute (sql);					#stauts=1 ，添加数据成功
			return status;
			#self.__conn.commit();
		except MySQLdb.Error,e:
			return "Mysql Error %d: %s" % (e.args[0], e.args[1]);	
		
#初始化类

db = {
	'user':'root', 
	'db':'student',
	'passwd':'root', 
	'host':'localhost',
	'charset':'utf8' 
	}; 

mysql = Mysql( db );		
mysql.conn();
#print( mysql.charset );