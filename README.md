# SQL Statements Parser [2018-11-30]
用Python解析输入的SQL语句，实现选择运算（表扫描和索引扫描 **[B+树索引]**）和连接运算（嵌套循环连接、归并连接和散列连接），并给出运行时间，比较不同算法的优劣。

## 1. 数据集
**authors.txt** : Author_ID, Author_Name, Sex, Age

eg: [105992	郝永梅	女	35]

**journals.txt** : Journal_ID, Journal_Name, City, Class

eg: [1	语言文字应用	北京市	A]

**papers.txt** : Paper_ID, Title, Author_Name, Journal_Name, Year, Content

eg: [11	hph7ekvaru2f6hc	邓世州	北方音乐	2011	p7j94rgbb1l1zpnqx0f9f5v89cju36ovpsojjoutqbctgomsb2rw8qel6cykt7gq4vrbe0l0og8kvj8x6p23yj74k1umnvk1p0v2;w6bchpr2t0lhvp4keeni4uboxnynrxd7klejr7pr61aa8g7bopduuuo5xg6wbnvmbez903ktzhagt6fzpfozoo7aox1t4enkfbat;o09a9pqt3w2qevvkeiqhjn0afi121njru1m4bodxw0efpq56t3fhqi5tqo72sp1ihipa12xj7xjn144yzg4xbst6yvzaexlatr9q	3qlrt4tocef4l4rvtnufn5v2rz0r61aakuy2rwbardwtp2kh1nkctb53d20twcsjvcfj8lrra3j5wyscqn2i68hcz5bbhumpch7s
]

## 2. 输入语句说明
- 支持子句select,from,where
- 支持大写、小写
- 支持任意多的换行、空格、制表符，但要求必须以‘;’结束
- 支持select子句中有distinct、*、或自定义属性名的输入，但不能在属性名前加[表名.]
- 支持from子句中有一个表
- 支持where子句中有多个查询条件，目前支持的条件类型为>、<、=、<>、>=、<=

eg:
[
select journals.id, journal, title
from papers,journals
where  journals.name = papers.journal  and papers.id<500 and class = 'A';
]

### （1） 解析select子句
```python
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
```
### (2) 解析from子句
```python
#处理from子句，主要需要在表名后面加上'.txt'，方便后面读取文件
def processFrom():
    from_sql=dict_sql['from']
    table_name=[]
    for i in range(0,len(from_sql)):
        table_name.append(from_sql[i]+'.txt')
    return table_name
```
### (3) 解析where子句
```python
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
#得到where子句需要比较的实体 
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
```

## 3. 功能描述
### （1） 选择运算
- 表扫描（Table Scan)：输入SQL语句，从头到尾扫描表，找到符合条件的数据
```python
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
```
- 索引扫描（Index Scan）：首先先对数据集建立B+树索引，输入SQL语句，然后根据索引找到符合条件的数据
```python
建立B+树详情见代码index_scan.py
```

### （2） 连接运算
- 嵌套循环连接
**思路**：*一次读取两个表中的一个元组，模拟内存中只能放下两个表中各一个元组的情况。进行条件的判断：若terms中table1和table2的表一样，则进行如表扫描方式的判断即可；若两个属性对应的表不一样，则需要将两个表中对应属性的值拿出来比较，若满足条件则flag+=1.最后满足所有条件的两个表中select语句要求的属性的值存入result中。*

```python
#读取文件,连接两个表
file0=open(table_name[0],encoding='UTF-8-sig')
file1=open(table_name[1],encoding='UTF-8-sig')
line0=file0.readline()
line1=file1.readline()
l0=line0
l1=line1
dict_line={table_name[0]:line0,table_name[1]:line1}#模拟内存中一次只能放下两张表中各一个元组，key为表名，value为对应元组
result=pd.DataFrame(columns=([selects['attr'][i] for i in range(0,len(select_name))]))#结果存到result中
cnt=0
test=0
endflag=False
while l0:
    if endflag == True:
        break
    line0=line0.strip().split("\t")
    line0[0]=int(line0[0])
    dict_line[table_name[0]]=line0
    while l1:
        if endflag == True:
            break
        flag=0
        line1=line1.strip().split("\t")
        line1[0]=int(line1[0])
        dict_line[table_name[1]]=line1
        for i in range(0,len(terms)):#判断两个元组是否满足所有条件
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
                else:#处理等值连接属性
                    t_line2=dict_line[terms['table2'][i]]

                    if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] == t_line2[attr_to_int[terms['table2'][i]][terms['second_term'][i]]]:
                        #print("t_line2: ",t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]])
                        flag+=1
            elif terms['char'][i]==4:
                if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] >= terms['second_term'][i]:
                    flag+=1
            elif terms['char'][i]==5:
                if t_line[attr_to_int[terms['table1'][i]][terms['first_term'][i]]] <= terms['second_term'][i]:
                    flag+=1
        if flag==len(terms):#如果所有条件都满足，就将两张表中select中对应的属性的值加入result
            t_arow=[]
            for j in range(0,len(select_name)):
                t_arow.append(dict_line[selects['table'][j]][attr_to_int[selects['table'][j]][selects['attr'][j]]])
            result.loc[cnt]=[t_arow[k] for k in range(0,len(t_arow))]
            cnt+=1
            if cnt>=2:
                endflag=True
        line1=file1.readline()
        l1=line1
    file1.close()
    file1=open(table_name[1],encoding='UTF-8-sig')
    line1=file1.readline()
    l1=line1 
    line0=file0.readline()
    l0=line0
#结果写出到csv
if distinct==True:
    result=result.drop_duplicates()#进行distinct操作
    result.index=[k for k in range(0,len(result))]#去重后重新给行索引编号
result.to_csv("result_loop_join.csv",mode='a',encoding='UTF-8')
timeEnd=time.time()#结束计时
runTime=float('%.2f'%(timeEnd-timeStart))
print("查询已成功执行，结果请看result_loop_join.csv文件")
print("查询用时",runTime,"s，查询结果返回",len(result),"行")
```
- 归并连接
**思路**：*首先先对两个表按照连接属性进行升序排列。然后循环遍历两个表，若外表的连接属性小于内表，则外表指针加一；若内表的连接属性小于外表，则内表指针加一。若连接属性值相等，则进行下一步判断，看是否满足所有的条件。将满足所有条件的元组对应select要求的元组加入result中。*
```python
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
if table_name[0]=='journals.txt':
    name2='journal'
elif table_name[0]=='authors.txt':
    name2='author'
#先对两个表按照连接属性排序,默认升序排列
table1=table1.sort_values(by='name')
table2=table2.sort_values(by=name2)
#注意DataFrame的行索引在排序后也改变了，因此要重新将行索引变为1-cnt1
table1.index=[k1 for k1 in range(1,cnt1)]
table2.index=[k2 for k2 in range(1,cnt2)]
i_1=1
i_2=1
cnt=1
while i_1 < cnt1:#结束条件：读到文件末尾
    if i_2 == cnt2:#如果第二个表已经读完也结束
        break
    while i_2 < cnt2:
        if table1['name'][i_1] < table2[name2][i_2]:#若左边表的连接值比右边表小，则跳过左边的表
            i_1+=1
        elif table1['name'][i_1] > table2[name2][i_2]:#若右边表的连接值比左边小，则跳过右边的表
            i_2+=1
        else:#若左边的表的连接值与右面的相等，则比较其他条件
            line0=[]
            line1=[]
            for ii in range(0,len(dict_data[table_name[0]])):
                line0.append(table1[dict_data[table_name[0]][ii]][i_1])
            for jj in range(0,len(dict_data[table_name[1]])):
                line1.append(table2[dict_data[table_name[1]][jj]][i_2])
            dict_line={table_name[0]:line0,table_name[1]:line1}#读入连接值相等的两个表的元组
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
            if flag==len(terms):#如果所有条件都满足，就将两张表中select中对应的属性的值加入result
                t_arow=[]
                for j_t in range(0,len(select_name)):
                    t_arow.append(dict_line[selects['table'][j_t]][attr_to_int[selects['table'][j_t]][selects['attr'][j_t]]])
                result.loc[cnt]=[t_arow[k] for k in range(0,len(t_arow))]
                cnt+=1
            i_2+=1
#结果写出到csv
if distinct==True:
    result=result.drop_duplicates()#进行distinct操作
    result.index=[k for k in range(0,len(result))]#去重后重新给行索引编号
result.to_csv("result_merge_join.csv",mode='a',encoding='UTF-8')
timeEnd=time.time()#结束计时
runTime=float('%.2f'%(timeEnd-timeStart))
print("查询已成功执行，结果请看result_merge_join.csv文件")
print("查询用时",runTime,"s，查询结果返回",len(result),"行")
```
- 散列连接
**思路**：*将哈希表设为字典，key为哈希桶的编号，value为哈希到该桶里的元组的编号（即行索引）。然后对两个表进行扫描，将name和journal/author属性列上的值进行哈希。遍历两个哈希桶，若两个哈希表对应的同一个编号的哈希桶中有一个为空，则跳过，因为不存在这样的元组。若都存在，但连接条件不等，则跳过。注意由于哈希桶中可能有多个（一般不超过3个）元组编号，因此要遍历每一个哈希桶中的元素去读取，将元组编号存入dict_line中表名对应的列表中。最后对于每一组连接条件相等的元组，判断其是否满足所有的条件，将满足条件的元组对应的select中的属性列的值加入result中。*
```python
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
```
