# SQL-Statements-Parser
用Python解析输入的SQL语句，实现选择运算（表扫描和索引扫描）和连接运算（嵌套循环连接、归并连接和散列连接），并给出运行时间

## 1.数据集
**authors.txt** : Author_ID, Author_Name, Sex, Age
**journals.txt** : Journal_ID, Journal_Name, City, Class
**papers.txt** : Paper_ID, Organization, Author_Name, Journal_Name, Yaer

## 2.输入语句说明
- 支持子句select,from,where
- 支持大写、小写
- 支持任意多的换行、空格、制表符，但要求必须以‘;’结束
- 支持select子句中有distinct、*、或自定义属性名的输入，但不能在属性名前加[表名.]
- 支持from子句中有一个表
- 支持where子句中有多个查询条件，目前支持的条件类型为>、<、=、<>、>=、<=


## 3.功能描述
### （1）选择运算
- 表扫描（Table Scan)：输入SQL语句，从头到尾扫描表，找到符合条件的数据
- 索引扫描（Index Scan）：首先先对数据集建立B+树索引，输入SQL语句，然后根据索引找到符合条件的数据

### （2）连接运算
- 嵌套循环连接
- 归并连接
- 散列连接
