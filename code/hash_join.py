# -*- coding: UTF-8 -*-
#实现两个表的哈希连接
#哈希连接中，将两个表连接的属性进行哈希，两个哈希表的长度相同
#分别遍历两个哈希表，对同一行的桶，当两个桶都不为0时进行连接，判断符合条件后放入结果集中
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
#where子句中目前只处理>,<,=,<>
char_in_where={'<>':0,'>':1,'<':2,'=':3}
#属性名转换成数字
attr_to_int={'authors.txt':{'id':0,'name':1,'sex':2,'age':3}, 
            'journals.txt':{'id':0,'name':1,'addr':2,'class':3},
            'papers.txt':{'id':0,'title':1,'author':2,'journal':3,'year':4,'keyword':5,'org':6}}
table_to_int={'authors.txt':1,'journals.txt':0,'papers.txt':2}#按照表的大小排列
hashNum={'authors.txt':12281,'journals.txt':12281,'papers.txt':12281}#按照PPT，哈希表大小应该一样
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

def getDictSql():
    part_sql=sql
    currentTag='select'
    while True:
        if currentTag == ';':
            break
        preTag=currentTag
        part_sql=part_sql[part_sql.find(currentTag,0):]
        currentTag=nextStmtTag(part_sql,currentTag)
        child_sql=part_sql[part_sql.find(preTag,0)+len(preTag):part_sql.find(currentTag,0)]#两个关键词之间的句子
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
    table_name.sort()#little trick 表顺序确定方便后面处理
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
        for tt in range(0,len(table_name)):#需要遍历所有的表名
            for i in range(0,len(dict_data[table_name[tt]])):
                dict_sql['select'].append(dict_data[table_name[tt]][i])
    for j in range(0,len(dict_sql['select'])):
        if dict_sql['select'][j].find('distinct')!=-1:#处理distinct
            distinct=True
            dict_sql['select'][j]=(dict_sql['select'][j][dict_sql['select'][j].find('distinct')+8:]).strip()
    return dict_sql['select'],distinct

def getTerms(terms,table_name):
    for i in range(0,len(term_name)):
        if term_name[i].find('<>')!=-1:
            first_term=term_name[i][:term_name[i].find('<>')].strip()
            second_term=term_name[i][term_name[i].find('<>')+2:].strip()
            c=0
        elif term_name[i].find('>=')!=-1:
            first_term=term_name[i][:term_name[i].find('>=')].strip()
            second_term=term_name[i][term_name[i].find('>=')+2:].strip()
            c=4
        elif term_name[i].find('<=')!=-1:
            first_term=term_name[i][:term_name[i].find('<=')].strip()
            second_term=term_name[i][term_name[i].find('<=')+2:].strip()
            c=5
        elif term_name[i].find('>')!=-1:
            first_term=term_name[i][:term_name[i].find('>')].strip()
            second_term=term_name[i][term_name[i].find('>')+1:].strip()
            c=1
        elif term_name[i].find('<')!=-1:
            first_term=term_name[i][:term_name[i].find('<')].strip()
            second_term=term_name[i][term_name[i].find('<')+1:].strip()
            c=2
        elif term_name[i].find('=')!=-1:
            first_term=term_name[i][:term_name[i].find('=')].strip()
            second_term=term_name[i][term_name[i].find('=')+1:].strip()
            c=3
        #认为连接的条件为等值连接，且显式地表示出来
        #当操作符为=时且两端属性所属的表不是一个表时，认为这个是连接的属性
        if c!=3:
            if first_term.find('.') != -1:#有表名限制
                table1=first_term[:first_term.find('.')].strip()+'.txt'
                first_term=first_term[first_term.find('.')+1:]
            else:
                for j in range(0,len(table_name)):
                    if first_term in dict_data[table_name[j]]:
                        table1=table_name[j]
            table2=table1
        #对于其他属性，一般认为操作符两端的属性或常量都属于同一张表
        else:
            #寻找表名
            if first_term.find('.') != -1:#有表名限制
                table1=first_term[:first_term.find('.')].strip()+'.txt'
                first_term=first_term[first_term.find('.')+1:]
                table2=table1
                if second_term.find('.')!=-1:
                    table2=second_term[:second_term.find('.')].strip()+'.txt'
                    second_term=second_term[second_term.find('.')+1:]
                else:
                    for k in range(0,len(table_name)):
                        if second_term in dict_data[table_name[k]]:
                            table2=table_name[k]
            else:#无表名限制
                for j in range(0,len(table_name)):
                    if first_term in dict_data[table_name[j]]:
                        table1=table_name[j]
                table2=table1
                if second_term.find('.')!=-1:
                    table2=second_term[:second_term.find('.')].strip()+'.txt'
                    second_term=second_term[second_term.find('.')+1:]
                else:
                    for k in range(0,len(table_name)):
                        if second_term in dict_data[table_name[k]]:
                            table2=table_name[k]
        if first_term=='id':
            second_term=int(second_term)
        #print("i: ",i," first_term: ",first_term, " second_term: ",second_term)
        terms.loc[i]=[first_term,c,second_term,table1,table2]
    return terms

def getSelects(selects,table_name):
    for i in range(0,len(select_name)):
        if select_name[i].find('.')!=-1:
            table=select_name[i][:select_name[i].find('.')].strip()+'.txt'
            attr=select_name[i][select_name[i].find('.')+1:].strip()
        else:
            attr=select_name[i]
            for j in range(0,len(table_name)):
                if select_name[i] in dict_data[table_name[j]]:
                    table=table_name[j]
        selects.loc[i]=[attr,table]
    return selects

def HashFunction(name, numHash):#哈希函数，传入name表示连接属性的值，numHash表示哈希桶的个数
    key=hash(name)#利用python自带的hash函数，将字符串哈希成大整数（32位整数，有负数）
    #在网上找的Redis常用的哈希函数，处理32位整数的哈希情况，冲突率比较低
    key+=~(key<<15)
    key=key^(key>>10)
    key+=(key<<3)
    key^=(key>>6)
    key+=~(key<<11)
    key^=(key>>16)
    return key%numHash#最后取模，将值放在哈希桶里

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
print("查询的表名为：",table_name)
#分析查询子句‘where’
term_name=processWhere()#获得and之间的子句,目前只处理>,<,=,<>
#分析查询子句‘and’
select_name,distinct=processSelect()
print("查询的属性为：",select_name,distinct)
#将where中的条件再次细分，存到一个DataFrame中
#注意将id的值转为int,否则比大小判断时有问题
#要给条件first_term加上表名
#连接条件一定涉及两个表,且连接时一定是=
terms=pd.DataFrame(columns=('first_term','char','second_term','table1','table2'))
terms=getTerms(terms,table_name)
print("查询的条件为：")
print(terms)
#select中也要区分表名
selects=pd.DataFrame(columns=('attr','table'))
selects=getSelects(selects,table_name)
print("区分表名后查询的属性为：")
print(selects)

#读取文件
#连接两个表（先默认第二个表为paper表）
result=pd.DataFrame(columns=([selects['attr'][i] for i in range(0,len(select_name))]))#columns形式为列表！！！

file0=open(table_name[0],encoding='UTF-8-sig')
file1=open(table_name[1],encoding='UTF-8-sig')
line0=file0.readline()
line1=file1.readline()
table1=pd.DataFrame(columns=(dict_data[table_name[0]]))
table2=pd.DataFrame(columns=(dict_data[table_name[1]]))
cnt1=1
cnt2=1
print("读取数据中，请稍后.....")
while line0:
    line0=line0.strip().split('\t')
    line0[0]=int(line0[0])
    table1.loc[cnt1]=[line0[k] for k in range(0,len(line0))]#假设dataframe中cnt1的位置表示该元组的物理存储位置
    cnt1+=1
    line0=file0.readline()
while line1:
    line1=line1.strip().split('\t')
    line1[0]=int(line1[0])
    table2.loc[cnt2]=[line1[k] for k in range(0,len(line1))]#假设dataframe中cnt2的位置表示该元组的物理存储位置
    cnt2+=1
    line1=file1.readline()
print("读取结束！两个表的大小分别是：",cnt1, "行、",cnt2,"行")
print("\n正在执行查询，请稍后......\n")
timeStart=time.time()#开始计时
#进行哈希
#确定哈希桶个数
hashNum1=hashNum[table_name[0]]
hashNum2=hashNum[table_name[1]]
hashValue1={}#哈希桶，每个桶里存下对应元组的行序号
hashValue2={}#key为哈希桶的编号，value为哈希到该桶里的元组的编号（即行索引）
#初始化哈希表，每一个value初始化为空列表
for i in range(0,hashNum1):
    t=[]
    hashValue1[i]=t
for j in range(0,hashNum2):
    t=[]
    hashValue2[j]=t
#对第一个表的name进行哈希
for i in range(1,cnt1):
    value=HashFunction(table1['name'][i],hashNum1)
    hashValue1[value].append(i)
#确定第二表的属性列名
if table_name[0]=='journals.txt':
    name2='journal'
elif table_name[0]=='authors.txt':
    name2='author'
#对第二个表(papers)的author或journal进行hash
for i in range(1,cnt2):
    value=HashFunction(table2[name2][i],hashNum2)
    hashValue2[value].append(i)
    
# 哈希后的结果写入文件，方便查看
# file_out1=open("hashValue1.txt",mode='a',encoding='UTF-8')
# file_out2=open("hashValue2.txt",mode='a',encoding='UTF-8')
# for i in range(0,len(hashValue1)):
#     file_out1.write(str(i)+',')
#     for j in range(0,len(hashValue1[i])):
#         file_out1.write(str(hashValue1[i][j])+' ')
#     file_out1.write('\n')
# file_out1.close()
# for i in range(0,len(hashValue2)):
#     file_out2.write(str(i)+',')
#     for j in range(0,len(hashValue2[i])):
#         file_out2.write(str(hashValue2[i][j])+' ')
#     file_out2.write('\n')
# file_out2.close()
# print("write hash value finish")

#对两个哈希表进行扫描
cnt=0
for i_h in range(0,hashNum['papers.txt']):
    #print("            i=",i)
    if hashValue1[i_h] is None:
        pass
    if hashValue2[i_h] is None:
        pass
    for j in range(0,len(hashValue1[i_h])):
        for k in range(0,len(hashValue2[i_h])):
            index1=hashValue1[i_h][j]
            index2=hashValue2[i_h][k]
            if table1['name'][index1]!=table2[name2][index2]:#连接条件不相等，pass
                pass
            line0=[]
            line1=[]
            for ii in range(0,len(dict_data[table_name[0]])):
                line0.append(table1[dict_data[table_name[0]][ii]][index1])
            for jj in range(0,len(dict_data[table_name[1]])):
                line1.append(table2[dict_data[table_name[1]][jj]][index2])
            dict_line={table_name[0]:line0,table_name[1]:line1}
            flag=0
            for i in range(0,len(terms)):
                t_line=dict_line[terms['table1'][i]]
                if terms['char'][i]==0:
                    if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] != terms['second_term'][i]:
                        flag+=1
                elif terms['char'][i]==1:
                    if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] > terms['second_term'][i]:
                        flag+=1
                elif terms['char'][i]==2:
                    if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] < terms['second_term'][i]:
                        flag+=1
                elif terms['char'][i]==3:
                    if (terms['table1'][i]==terms['table2'][i]):
                        if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] == terms['second_term'][i]:
                            flag+=1
                    else:
                        t_line2=dict_line[terms['table2'][i]]
                        if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] == t_line2[attr_to_int[terms['table2'][i]][terms['second_term'][i]]]:
                            flag+=1
                elif terms['char'][i]==4:
                    if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] >= terms['second_term'][i]:
                        flag+=1
                elif terms['char'][i]==5:
                    if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] <= terms['second_term'][i]:
                        flag+=1
            if flag==len(terms):
                t_arow=[]
                for j_t in range(0,len(select_name)):
                    t_arow.append(dict_line[selects['table'][j_t]][attr_to_int[selects['table'][j_t]][selects['attr'][j_t]]])
                result.loc[cnt]=[t_arow[k] for k in range(0,len(t_arow))]
                cnt+=1
#结果写出到csv
if distinct==True:
    result=result.drop_duplicates()#进行distinct操作
    result.index=[k for k in range(0,len(result))]#去重后重新给行索引编号
result.to_csv("result_hash_join.csv",mode='a',encoding='UTF-8')
timeEnd=time.time()#结束计时
runTime=float('%.2f'%(timeEnd-timeStart))
print("查询已成功执行，结果请看result_hash_join.csv文件")
print("查询用时",runTime,"s，查询结果返回",len(result)-4,"行")
