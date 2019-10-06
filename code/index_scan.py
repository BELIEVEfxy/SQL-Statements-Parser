# -*- coding:utf-8 -*-
#索引扫描
import sys
import io
#import index
from random import randint, choice
from bisect import bisect_right, bisect_left
from collections import deque
import re
import numpy as np
import pandas as pd
import os
import math
import csv
from pandas.core.frame import DataFrame
import time

#SQL语句标志词
stmtTag = ['select','from','where','group by','order by',';']
#模拟数据字典
dict_data={'authors.txt':['id','name','sex','age'],
            'journals.txt':['id','name','addr','class'],
            'papers.txt':['id','title','author','journal','year','keyword','org']}
#where子句中目前只处理>,<,=,<>,>=,<=
char_in_where={'<>':0,'>':1,'<':2,'=':3,'>=':4,'<=':5}
charinwhere={0:'<>',1:'>',2:'<',3:'=',4:'>=',5:'<='}
#属性名转换成数字
attr_to_int={'authors.txt':{'id':0,'name':1,'sex':2,'age':3}, 
            'journals.txt':{'id':0,'name':1,'addr':2,'class':3},
            'papers.txt':{'id':0,'title':1,'author':2,'journal':3,'year':4,'keyword':5,'org':6}}
TABLES = {'authors':['id', 'name', 'sex', 'age'],
		'journals':['id', 'name', 'addr', 'class'], 
		'papers':['id', 'title', 'author', 'journal', 'year', 'keyword', 'org']}
OPERATORS = ['=', '>', '>=', '<', '<=', '<>']
CONJUNCTIONS = ['and', 'or']


#建立B+树索引
class InitError(Exception):
    pass

class ParaError(Exception):
    pass

#生成键值对
class KeyValue(object):
    __slots__=('key', 'value')
    def __init__(self, key, value):
        self.key=int(key) #一定要保证键值是整型
        self.value=value
    def __str__(self):
        return str((self.key, self.value))
    def __cmp__(self, key):
        if self.key>key:
            return 1
        elif self.key < key:
            return -1
        else:
            return 0
    def __lt__(self, other):
        if (type(self) == type(other)):
            return self.key < other.key
        else:
            return int(self.key) < int(other)
    def __eq__(self, other):
        if (type(self) == type(other)):
            return self.key == other.key
        else:
            return int(self.key) == int(other)
    def __gt__(self, other):
        return not self < other
class Bptree(object):
    class __InterNode(object):
        def __init__(self, M):
            if not isinstance(M, int):
                raise InitError('M must be int')
            if M <= 3:
                raise InitError('M must be greater then 3')
            else:
                self.__M = M
                self.clist = [] #存放区间
                self.ilist = [] #存放索引/序号
                self.par = None
        def isleaf(self):
            return False
        def isfull(self):
            return len(self.ilist)>=self.M-1
        def isempty(self):
            return len(self.ilist)<=(self.M+1)/2-1
        @property
        def M(self):
            return self.__M
 
    #叶子
    class __Leaf(object):
        def __init__(self,L):
            if not isinstance(L,int):
                raise InitError('L must be int')
            else:
                self.__L = L
                self.vlist = []
                self.bro = None #兄弟结点
                self.par = None #父结点
        def isleaf(self):
            return True
        def isfull(self):
            return len(self.vlist)>self.L
        def isempty(self):
            return len(self.vlist)<=(self.L+1)/2
        @property
        def L(self):
            return self.__L

    #初始化
    def __init__(self,M,L):
        if L>M:
            raise InitError('L must be less or equal then M')
        else:
            self.__M = M
            self.__L = L
            self.__root = Bptree.__Leaf(L)
            self.__leaf = self.__root
    @property
    def M(self):
        return self.__M
    @property
    def L(self):
        return self.__L
		
    #插入
    def insert(self, key_value):
        node = self.__root
        def split_node(n1):
            mid = self.M//2 #此处注意，可能出错
            newnode = Bptree.__InterNode(self.M)
            newnode.ilist = n1.ilist[mid:]
            newnode.clist = n1.clist[mid:]
            newnode.par = n1.par
            for c in newnode.clist:
                c.par = newnode
            if n1.par is None:
                newroot = Bptree.__InterNode(self.M)
                newroot.ilist = [n1.ilist[mid-1]]
                newroot.clist = [n1,newnode]
                n1.par=newnode.par = newroot
                self.__root = newroot
            else:
                i = n1.par.clist.index(n1)
                n1.par.ilist.insert(i,n1.ilist[mid-1])
                n1.par.clist.insert(i+1,newnode)
            n1.ilist = n1.ilist[:mid-1]
            n1.clist = n1.clist[:mid]
            return n1.par

        def split_leaf(n2):
            mid = (self.L + 1) // 2
            newleaf = Bptree.__Leaf(self.L)
            newleaf.vlist = n2.vlist[mid:]
            if n2.par == None:
                newroot = Bptree.__InterNode(self.M)
                newroot.ilist = [n2.vlist[mid].key]
                newroot.clist = [n2,newleaf]
                n2.par = newleaf.par = newroot
                self.__root = newroot
            else:
                i = n2.par.clist.index(n2)
                n2.par.ilist.insert(i,n2.vlist[mid].key)
                n2.par.clist.insert(i+1,newleaf)
                newleaf.par = n2.par
            n2.vlist = n2.vlist[:mid]
            n2.bro = newleaf
        def insert_node(n):
            if not n.isleaf():
                if n.isfull():
                    insert_node(split_node(n))
                else:
                    p=bisect_right(n.ilist,key_value)
                    insert_node(n.clist[p])
            else:
                p=bisect_right(n.vlist,key_value)
                n.vlist.insert(p,key_value)
                if n.isfull():
                    split_leaf(n)
                else:
                    return
        insert_node(node)

    #搜索
    def search(self, mi=None, ma=None):
        result=[]
        node=self.__root
        leaf=self.__leaf
        if mi is None or ma is None:
            raise ParaError('you need to setup searching range')
        elif mi > ma:
            raise ParaError('upper bound must be greater or equal than lower bound')
        def search_key(n,k):
            if n.isleaf():
                p=bisect_left(n.vlist,k)
                return (p,n)
            else:
                p=bisect_right(n.ilist,k)
                return search_key(n.clist[p],k)
        if mi is None:
            while True:
                for kv in leaf.vlist:
                    if kv<=ma:
                        result.append(kv)
                    else:
                        return result
                if leaf.bro==None:
                    return result
                else:
                    leaf=leaf.bro
        elif ma is None:
            index,leaf=search_key(node,mi)
            result.extend(leaf.vlist[index:])
            while True:
                if leaf.bro==None:
                    return result
                else:
                    leaf=leaf.bro
                    result.extend(leaf.vlist)
        else:
            if mi==ma:
                i,l=search_key(node,mi)
                try:
                    if l.vlist[i]==mi:
                        result.append(l.vlist[i])
                        return result
                    else:
                        return result
                except IndexError:
                    return result
            else:
                i1,l1=search_key(node,mi)
                i2,l2=search_key(node,ma)
                if l1 is l2:
                    if i1==i2:
                        return result
                    else:
                        result.extend(l2.vlist[i1:i2])
                        return result
                else:
                    result.extend(l1.vlist[i1:])
                    l=l1
                    while True:                        
                        if l.bro==l2:
                            result.extend(l2.vlist[:i2])
                            return result
                        elif l.bro != None:
                            result.extend(l.bro.vlist)
                            l=l.bro
                        else:
                            return result
    def traversal(self):
        result=[]
        l=self.__leaf
        while True:
            result.extend(l.vlist)
            if l.bro==None:
                return result
            else:
                l=l.bro

    def show(self):
        print('this b+tree is:\n')
        q=deque()
        h=0
        q.append([self.__root,h])
        while True:
            try:
                w,hei=q.popleft()
            except IndexError:
                return
            else:
                if not w.isleaf():
                    print(w.ilist,'the height is',hei)
                    if hei==h:
                        h+=1
                    q.extend([[i,h] for i in w.clist])
                else:
                    print([(v.key,v.value) for v in w.vlist],'the leaf is,',hei)

    #删除        
    def delete(self,key_value):
        def merge(n,i):
            if n.clist[i].isleaf():
                n.clist[i].vlist=n.clist[i].vlist+n.clist[i+1].vlist
                n.clist[i].bro=n.clist[i+1].bro
            else:
                n.clist[i].ilist=n.clist[i].ilist+[n.ilist[i]]+n.clist[i+1].ilist
                n.clist[i].clist=n.clist[i].clist+n.clist[i+1].clist
            n.clist.remove(n.clist[i+1])
            n.ilist.remove(n.ilist[i])
            if n.ilist==[]:
                n.clist[0].par=None
                self.__root=n.clist[0]
                del n
                return self.__root
            else:
                return n
        def tran_l2r(n,i):
            if not n.clist[i].isleaf():
                n.clist[i+1].clist.insert(0,n.clist[i].clist[-1])
                n.clist[i].clist[-1].par=n.clist[i+1]
                n.clist[i+1].ilist.insert(0,n.ilist[i])
                n.ilist[i]=n.clist[i].ilist[-1]
                n.clist[i].clist.pop()
                n.clist[i].ilist.pop()
            else:
                n.clist[i+1].vlist.insert(0,n.clist[i].vlist[-1])
                n.clist[i].vlist.pop()
                n.ilist[i]=n.clist[i+1].vlist[0].key
        def tran_r2l(n,i):
            if not n.clist[i].isleaf():
                n.clist[i].clist.append(n.clist[i+1].clist[0])
                n.clist[i+1].clist[0].par=n.clist[i]
                n.clist[i].ilist.append(n.ilist[i])
                n.ilist[i]=n.clist[i+1].ilist[0]
                n.clist[i+1].clist.remove(n.clist[i+1].clist[0])
                n.clist[i+1].ilist.remove(n.clist[i+1].ilist[0])
            else:
                n.clist[i].vlist.append(n.clist[i+1].vlist[0])
                n.clist[i+1].vlist.remove(n.clist[i+1].vlist[0])
                n.ilist[i]=n.clist[i+1].vlist[0].key
        def del_node(n,kv):
            if not n.isleaf():
                p=bisect_right(n.ilist,kv)
                if p==len(n.ilist):
                    if not n.clist[p].isempty():
                        return del_node(n.clist[p],kv)
                    elif not n.clist[p-1].isempty():
                        tran_l2r(n,p-1)
                        return del_node(n.clist[p],kv)
                    else:
                        return del_node(merge(n,p),kv)
                else:
                    if not n.clist[p].isempty():
                        return del_node(n.clist[p],kv)
                    elif not n.clist[p+1].isempty():
                        tran_r2l(n,p)
                        return del_node(n.clist[p],kv)
                    else:
                        return del_node(merge(n,p),kv)
            else:
                p=bisect_left(n.vlist,kv)
                try:
                    pp=n.vlist[p]
                except IndexError:
                    return -1
                else:
                    if pp!=kv:
                        return -1
                    else:
                        n.vlist.remove(kv)
                        return 0
        del_node(self.__root,key_value)

def getNum(x, table):
    i = 0
    for attribute in TABLES[table]:
        if x == attribute:
            return i
        i += 1

def createIndex(table, attribute_index):
    mybptree = Bptree(10, 10)
    f_file = (table + '.txt')
    key_ = getNum(attribute_index, table)
    attributes = TABLES[table]
    value_ = []
    for attribute in attributes:
        i = getNum(attribute, table)
        value_.append(i)
    with open(f_file, "r", encoding='UTF-8-sig') as f:
        for line in f:
            info = line.split()
            
            key = info[key_]
            value = []
            for x in value_:
                value.append(info[x])
            
            newnode = KeyValue(key, value)
            mybptree.insert(newnode)
    return mybptree

def searchIndex(bptree, conditions, key_):
    #查找操作
    mini = -99999999
    maxi = 99999999
    i = 0
    for condition in conditions:
        if condition[0] == key_:
            if (condition[1] == '<' and int(condition[2]) < maxi) :
                maxi = int(condition[2])
            elif (condition[1] == '<=' and (int(condition[2])+1) < maxi):
                maxi = int(condition[2]) + 1
            elif (condition[1] == '>' and (int(condition[2])+1) > mini):
                mini = int(condition[2]) + 1
            elif (condition[1] == '>=' and int(condition[2]) > mini):
                mini = int(condition[2])
            elif condition[1] == '=':
                mini = int(condition[2])
                maxi = int(condition[2]) + 1
        i += 1
    result = bptree.search(mini, maxi)
    return result
#B+树部分结束   
def rmNoUseChar(sql):
    while sql.find("'") != -1:#将引号删除，不论什么类型都当字符类型处理
        sql = sql.replace("'","")
    while sql.find('"') != -1:
        sql = sql.replace('"','')
    while sql.find('\t') != -1:#删除制表符
        sql = sql.replace("\t"," ")
    while sql.find('\n') != -1:#删除换行符
        sql = sql.replace("\n"," ")
    statements = sql.split(" ")#分割成列表，删除多余空格后在拼接成字符串
    while "" in statements:
        statements.remove("")
    sql=""
    for stmt in statements:
        sql += stmt+ " "
    return sql[0:-1]#最后一个空格删掉

def nextStmtTag(sql,currentTag):#根据当前标志词找下一个标志词
    index = sql.find(currentTag,0)
    for tag in stmtTag:
        if sql.find(tag,index+len(currentTag)) != -1:
            return tag
#由于python默认为回车结束，因此在；结束前，不断递归式的读取输入，将其全部拼接在sql之后。
#注意在读取非结束语句那一行时，要加上return，否则会因为t_sql是临时变量而无法将拼接后的结果返回到上一层，导致结果为None
def enterSelect(sql):#输入select语句
    t_sql=input()
    #大写字母转为小写字母，注意条件引号中的大写字母不能变
    new=[]
    for s in t_sql:
        new.append(s)
    i=0
    while i < len(new):
        if new[i]=="'":
            for j in range(i+1,len(new)):
                if new[j]=="'":
                    i=j+1
                    break
        new[i]=new[i].lower()
        i+=1
    t_sql=''.join(new)
    if(t_sql.find(';') != -1):#存在‘；’
        sql+=t_sql
        #print(sql)
        return sql
    else:
        sql+=t_sql
        return (enterSelect(sql))#注意加上return，否则递归后返回值为None
#将语句分解，存到字典中，key为select/from/where，value为相应子句后的值
#stmtTag储存关键词标志，为['select','from','where','group by','order by',';']
#通过找到下一个关键词来获得本关键词和下关键字之间的子句
def getDictSql():
    part_sql=sql
    currentTag='select'
    while True:
        if currentTag == ';':
            break
        preTag=currentTag
        part_sql=part_sql[part_sql.find(currentTag,0):]
        currentTag=nextStmtTag(part_sql,currentTag)
        child_sql=part_sql[part_sql.find(preTag,0)+len(preTag):part_sql.find(currentTag,0)]#获取两个关键词之间的句子
        arow=child_sql.strip(",").split(',')
        for i in range(0,len(arow)):
            arow[i]=arow[i].strip()
        dict_sql[preTag]=arow
    return dict_sql

#处理from子句，主要需要在表名后面加上'.txt'，方便后面读取文件
def processFrom():
    from_sql=dict_sql['from']
    table_name=[]
    for i in range(0,len(from_sql)):
        table_name.append(from_sql[i]+'.txt')
    return table_name,from_sql[0]
#处理where子句，通过and作为划分的标志，把and之间的条件分解
def processWhere():
    where_sql=dict_sql['where']
    and_num=0
    term_name=[]
    child_name=where_sql[0]
    while True:
        if(child_name.find('and')==-1):
            term_name.append(child_name.strip())
            break
        and_num += 1
        term_name.append(child_name[:child_name.find('and')].strip())
        child_name=child_name[child_name.find('and')+3:]
    return term_name,and_num
#解析select子句，主要需要处理*和distinct的情况
def processSelect():
    distinct=False
    if '*' in dict_sql['select']:#处理select中的*，将字典内容换成这个表中所有属性名
        dict_sql['select'].remove('*')
        for i in range(0,len(dict_data[table_name[0]])):
            dict_sql['select'].append(dict_data[table_name[0]][i])
    for j in range(0,len(dict_sql['select'])):
        if dict_sql['select'][j].find('distinct')!=-1:#处理distinct
            distinct=True
            dict_sql['select'][j]=(dict_sql['select'][j][dict_sql['select'][j].find('distinct')+8:]).strip()
    return dict_sql['select'],distinct
#进一步处理where中的条件
#根据标志符号>,<,=,<>,>=,<=，将每个条件划分为三部分——左属性、操作符、右条件
def getTerms(terms):
    for i in range(0,len(term_name)):
        if term_name[i].find('<>')!=-1:#处理含<>的条件
            first_term=term_name[i][:term_name[i].find('<>')].strip()
            second_term=term_name[i][term_name[i].find('<>')+2:].strip()
            if(first_term=='id'):
                second_term=int(second_term)
            terms.loc[i]=[first_term,0,second_term]
            continue
        elif term_name[i].find('>=')!=-1:#处理含>=的条件
            first_term=term_name[i][:term_name[i].find('>=')].strip()
            second_term=term_name[i][term_name[i].find('>=')+2:].strip()
            if(first_term=='id'):
                second_term=int(second_term)
            terms.loc[i]=[first_term,4,second_term]
            continue
        elif term_name[i].find('<=')!=-1:#处理含<=的条件
            first_term=term_name[i][:term_name[i].find('<=')].strip()
            second_term=term_name[i][term_name[i].find('<=')+2:].strip()
            if(first_term=='id'):
                second_term=int(second_term)
            terms.loc[i]=[first_term,5,second_term]
            continue
        elif term_name[i].find('>')!=-1:#处理含>的条件
            first_term=term_name[i][:term_name[i].find('>')].strip()
            second_term=term_name[i][term_name[i].find('>')+1:].strip()
            if(first_term=='id'):
                second_term=int(second_term)
            terms.loc[i]=[first_term,1,second_term]
            continue
        elif term_name[i].find('<')!=-1:#处理含<的条件
            first_term=term_name[i][:term_name[i].find('<')].strip()
            second_term=term_name[i][term_name[i].find('<')+1:].strip()
            if(first_term=='id'):
                second_term=int(second_term)
            terms.loc[i]=[first_term,2,second_term]
            continue
        elif term_name[i].find('=')!=-1:#处理含=的条件
            first_term=term_name[i][:term_name[i].find('=')].strip()
            second_term=term_name[i][term_name[i].find('=')+1:].strip()
            if(first_term=='id'):
                second_term=int(second_term)
            terms.loc[i]=[first_term,3,second_term]
            continue
    return terms


def getATCC(info):
	table = info[3]
	if table not in TABLES.keys():
		print('输入表格错误 重新输入')
		return -1,-1,-1,False
		
	if info[1] == '*':
		attributes = TABLES[table]
	else :
		attributes = info[1].split(',')
		for attribute in attributes:
			if attribute not in TABLES[table]:
				print('输入属性错误 重新输入')
				return -1,-1,-1,False
		
	conditions = []
	conjunctions = []
	if len(info) > 4:
		init_conditions = info[5:]
		i = 0
		for condition in init_conditions:
			if (condition in TABLES[table] and init_conditions[i + 1] in OPERATORS):  
				
				new_condition = [condition, init_conditions[i + 1], init_conditions[i + 2]]
				
				conditions.append(new_condition)
			elif (condition in TABLES[table] and init_conditions[i + 1] == 'between' and init_conditions[i + 3] == 'and'): 
				new_condition1 = [condition, '>', init_conditions[i + 2]]
				new_condition2 = [condition, '<', init_conditions[i + 4]]
				conditions.append(new_condition1)
				conditions.append(new_condition2)
				conjunctions.append('and')
			elif (condition in CONJUNCTIONS and init_conditions[i + 1] in TABLES[table]):
				conjunctions.append(condition)
			i += 1

	print('\nattributes :')
	for attribute in attributes:
		print('	', attribute)
	print('\ntable :')
	print('	', table)
	print('\nconditions :')
	for condition in conditions:
		print('	', condition)
	
	return attributes, table, conditions, conjunctions, True

def checkConditions(table, res, conditions, conjunctions):
	conjunctions.insert(0, 'and')
	check_res = True
	
	i_con = 0
	for condition in conditions: # XXX >= A
		if condition[0] in TABLES[table]:
			check_this_condition = True
			i = getNum(condition[0], table)
			
			if condition[1] == '<' and not int(res[i]) < int(condition[2]):
				check_this_condition = False
			elif condition[1] == '<=' and not int(res[i]) <= int(condition[2]):
				check_this_condition = False
			elif condition[1] == '>' and not int(res[i]) > int(condition[2]):
				check_this_condition = False
			elif condition[1] == '>=' and not int(res[i]) >= int(condition[2]):
				check_this_condition = False
			elif condition[1] == '<>' and not str(res[i]) != str(condition[2]):
				check_this_condition = False
			elif condition[1] == '=' and not str(res[i]) == str(condition[2]):
				check_this_condition = False
			if conjunctions[i_con] == 'and':
				check_res = check_res and check_this_condition
			elif conjunctions[i_con] == 'or':
				check_res = check_res or check_this_condition
			i_con += 1
	return check_res

def printResult(result, f_write, attributes, table):
	for i in range(0,len(select_name)):
		f_write.write(select_name[i]+',')
	f_write.write('\n')
	output = []
	for attribute in attributes:
		output.append(getNum(attribute, table))
	
	for v in result:
		i = 0
		for x in v.value:
			if i in output:
				f_write.write(str(x) + ',')
			i += 1
		f_write.write('\n')

#主函数
f_write = open('result_index_scan.csv',mode='w',encoding='UTF-8')
flag = 1
authors_bptree = Bptree(10, 10)
journals_bptree = Bptree(10, 10)
papers_bptree = Bptree(10, 10)	
authors_age_bptree = Bptree(10, 10)
flag_authors_bptree, flag_journals_bptree, flag_papers_bptree = 0, 0, 0
flag_authors_age_bptree = 0


#建立索引
startIndex=time.time()
print('\n建立索引中......')
attribute_index = 'id' # 需要建立索引的属性
#authors建立索引
bptree = authors_bptree
if flag_authors_bptree == 0:
    authors_bptree = createIndex('authors', attribute_index) 
    flag_authors_bptree = 1
bptree = authors_bptree

attribute_index = 'age' # 需要建立索引的属性

bptree_age = authors_age_bptree
if flag_authors_age_bptree == 0:
    authors_age_bptree = createIndex('authors', attribute_index) 
    flag_authors_age_bptree = 1
bptree_age = authors_age_bptree
endIndex=time.time()
used=float('%.2f'%(endIndex-startIndex))
print('\n索引建立完成! 用时',used,'s.')

#解析SQL
init_sql=''
print("请输入查询语句：")
sql=enterSelect(init_sql)
sql=rmNoUseChar(sql)
#建立字典，储存解析sql后的结果
dict_sql={}
dict_sql=getDictSql()
#分析查询子句‘from’
table_name,table=processFrom()#获得表完整名称列表
print("\n查询的表名为：",table_name)
#分析查询子句‘where’
term_name,and_num=processWhere()#获得and之间的子句,目前只处理>,<,=,<>
#print(term_name)
#分析查询子句‘and’
select_name, distinct=processSelect()
print("查询的属性为：",select_name," ",distinct)
#将where中的条件再次细分，存到一个DataFrame中
#注意将id的值转为int,否则比大小判断时有问题
terms=pd.DataFrame(columns=('first_term','char','second_term'))
terms=getTerms(terms)
print("查询的条件为：")
print(terms)
#get attributes
attributes=select_name
#get table

#get conditioin
conditions=[]
for i in range(0,len(terms)):
    t=[]
    t.append(terms['first_term'][i])
    t.append(charinwhere[terms['char'][i]])
    t.append(terms['second_term'][i])
    conditions.append(t)
#get conjunctions
conjunctions=[]
for j in range(0,and_num):
    conjunctions.append('and')
#get flag
flag=True
print("\n正在执行查询，请稍后......\n")
timeStart=time.time()#开始计时
# search index
check_id = 0
result = []
for condition in conditions:
    if condition[0] == 'id':
        result = searchIndex(bptree, conditions, 'id')
        check_id = 1
        break
if check_id == 0:
    result = searchIndex(bptree_age, conditions, 'age')

fin_result = []
for res in result:
    check_result = checkConditions(table, res.value, conditions, conjunctions)
    if check_result == True:
        fin_result.append(res)

result = fin_result

printResult(result, f_write, attributes, table)
f_write.close()
timeEnd=time.time()#结束计时
runTime=float('%.5f'%(timeEnd-timeStart))
print("查询已成功执行，结果请看result_index_scan.csv文件")
print("查询用时",runTime,"s，查询结果返回",len(result),"行")
#if __name__ == '__main__':

#	main()