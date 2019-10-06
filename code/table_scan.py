# -*- coding: UTF-8 -*-
#2018/11/28
#单个表，表扫描
import re
import numpy as np
import pandas as pd
import os
import math
import csv
from pandas.core.frame import DataFrame
import time

#select 子语句标志词
stmtTag = ['select','from','where','group by','order by',';']
#模拟数据字典
dict_data={'authors.txt':['id','name','sex','age'],
            'journals.txt':['id','name','addr','class'],
            'papers.txt':['id','title','author','journal','year','keyword','org']}
#where子句中目前只处理>,<,=,<>,>=,<=
char_in_where={'<>':0,'>':1,'<':2,'=':3,'>=':4,'<=':5}
#属性名转换成数字
attr_to_int={'authors.txt':{'id':0,'name':1,'sex':2,'age':3}, 
            'journals.txt':{'id':0,'name':1,'addr':2,'class':3},
            'papers.txt':{'id':0,'title':1,'author':2,'journal':3,'year':4,'keyword':5,'org':6}}
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
    return table_name
#处理where子句，通过and作为划分的标志，把and之间的条件分解
def processWhere():
    where_sql=dict_sql['where']
    term_name=[]
    child_name=where_sql[0]
    while True:
        if(child_name.find('and')==-1):
            term_name.append(child_name.strip())
            break
        term_name.append(child_name[:child_name.find('and')].strip())
        child_name=child_name[child_name.find('and')+3:]
    return term_name
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

#主函数：
init_sql=''
print("请输入查询语句：")
sql=enterSelect(init_sql)
sql=rmNoUseChar(sql)
#建立字典，储存解析sql后的结果
dict_sql={}
dict_sql=getDictSql()
#分析查询子句‘from’
table_name=processFrom()#获得表完整名称列表
print("\n查询的表名为：",table_name)
#分析查询子句‘where’
term_name=processWhere()#获得and之间的子句,目前只处理>,<,=,<>
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

print("\n正在执行查询，请稍后......\n")
timeStart=time.time()#开始计时
#读取文件
file=open(table_name[0],encoding='UTF-8-sig')#文件第一行不是id,name...等,UTF-8-sig是因为id前面出现了\ufeff,这是数据库导出时的问题
line=file.readline()
#查询结果存到result中
result=pd.DataFrame(columns=([select_name[i] for i in range(0,len(select_name))]))#columns形式为列表！！！
cnt=0
t=0
while line:
    arow=line.strip().split("\t")
    arow[0]=int(arow[0])
    flag=0#判断是否符合所有条件，符合条件为flag=len(terms)，满足一项条件，flag就加一
    for i in range(0,len(terms)):#所有的条件储存在terms，其数据结构为DataFrame
        if terms['char'][i]==0:
            if arow[attr_to_int[table_name[0]][terms['first_term'][i]]] != terms['second_term'][i]:
                flag+=1
        elif terms['char'][i]==1:
            if arow[attr_to_int[table_name[0]][terms['first_term'][i]]] > terms['second_term'][i]:
                flag+=1
        elif terms['char'][i]==2:
            if arow[attr_to_int[table_name[0]][terms['first_term'][i]]] < terms['second_term'][i]:
                flag+=1
        elif terms['char'][i]==3:
            if arow[attr_to_int[table_name[0]][terms['first_term'][i]]] == terms['second_term'][i]:
                flag+=1
        elif terms['char'][i]==4:
            if arow[attr_to_int[table_name[0]][terms['first_term'][i]]] >= terms['second_term'][i]:
                flag+=1
        elif terms['char'][i]==5:
            if arow[attr_to_int[table_name[0]][terms['first_term'][i]]] <= terms['second_term'][i]:
                flag+=1
    #符合条件的结果存入result中
    if flag == len(terms):#当该元组满足所有要求时
        t_arow=[]
        for j in range(0,len(select_name)):
            t_arow.append(arow[attr_to_int[table_name[0]][select_name[j]]])
        result.loc[cnt]=[arow[attr_to_int[table_name[0]][select_name[j]]] for j in range(0,len(select_name))]
        cnt+=1
    line=file.readline()
file.close()
#结果写出到csv
if distinct==True:
    result=result.drop_duplicates()#进行distinct操作
    result.index=[k for k in range(0,len(result))]#去重后重新给行索引编号
result.to_csv("result_table_scan.csv", mode='w', encoding = "utf-8")
timeEnd=time.time()#结束计时
runTime=float('%.2f'%(timeEnd-timeStart))
print("查询已成功执行，结果请看result_table_scan.csv文件")
print("查询用时",runTime,"s，查询结果返回",len(result),"行")




