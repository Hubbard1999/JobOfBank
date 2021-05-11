#!/usr/bin/env python
# coding: utf-8

# In[165]:


from clickhouse_driver import connect
import clickhouse_driver
import time


# In[166]:


import pandas as pd
import ast

randomsplit_params = pd.read_csv('/root/data/jenkins_job/randomsplit_params.csv')


# In[167]:


operation_id = int(randomsplit_params.columns[0])
targetUserId = randomsplit_params.columns[1]
str_dict = ""
for i in range(2,len(randomsplit_params.columns)):
    if(i==2):
        key = randomsplit_params.columns[i].split('{')[1].split(':')[0]
        value = randomsplit_params.columns[i].split('{')[1].split(':')[1]
        str_dict = str_dict +'{'+'\"'+ key + '\"' +":"+ value + ","
    elif(i>2 and i<len(randomsplit_params.columns)-1):
        key = randomsplit_params.columns[i].split(':')[0]
        value = randomsplit_params.columns[i].split(':')[1]
        str_dict = str_dict +'\"'+ key + '\"' +":"+ value + ","
    else:
        key = randomsplit_params.columns[i].split('}')[0].split(':')[0]
        value = randomsplit_params.columns[i].split('}')[0].split(':')[1]
        str_dict = str_dict +'\"'+ key + '\"' +":"+ value + '}'

dict = ast.literal_eval(str_dict)


# - 实现随机插入数据库的函数

# In[169]:


def randomInsert(n,list1,list2,number,sql):
    name = locals()
    if(number<=4):
        random = list1.pop(0)
        conn = connect(host='chi-ftabc-clickhouse-{}-0.default.svc.cluster.local'.format(random), port='9000', database='ftabcch', user='ftabc', password='aihub@2020')
        cursor = conn.cursor()
            
        cursor.execute(sql)
        cursor.fetchall()
        time.sleep(1)
        conn.close()
        cursor.close()
    else:
        random = list2.pop(0)
        conn = connect(host='chi-ftabc-clickhouse-{}-0.default.svc.cluster.local'.format(random), port='9000', database='ftabcch', user='ftabc', password='aihub@2020')
        cursor = conn.cursor()
            
        cursor.execute(sql)
        cursor.fetchall()
        time.sleep(1)
        conn.close()
        cursor.close()


# In[170]:


import math

def get_group(slice,operationid,targetid,doc):
    percent = []
    columns = []
    user_id = []
    for k in doc:
        doc[k] = doc[k]/100
        columns.append(k)
        percent.append(doc[k])
    num = len(columns)
    conn = connect(host='chi-ftabc-clickhouse-0-0.default.svc.cluster.local', port='9000', database='ftabcch', user='ftabc', password='aihub@2020')
    cursor = conn.cursor()
    cursor.execute("select distinct id from ads_user_tag_distribute where tag_id={}".format(targetid))
    tag_id_num = len(cursor.fetchall())
    import random
    list_part = [i for i in range(slice)]
    random.shuffle(list_part)
    import math
    n = math.ceil(num/slice)
    many_list_part = list_part*n
    list_all = []
    for i in range(1,n+1):
        name = locals()
        name['list'+str(i)] = many_list_part[(i-1)*4:(i-1)*4+4]
        random.shuffle(name['list'+str(i)])
        for x in (name['list'+str(i)]):
            list_all.append(x)
    group_number = []
    name = locals()
    for i in range(num):
        if(percent[i]==0):
            name = locals()
            name['part'+str(i)] = """insert into add_plan_user_group (operation_id, group_name, group_user)
            select {} as operation_id, '{}' as group_name, [0] as group_user""".format(operationid,columns[i])
            
            randomInsert(n,list_part,list_all,num,name['part'+str(i)])
            group_number.append(0)
            
        else:
            sql = """insert into ads_plan_user_group (operation_id, group_name, group_user) select {} as operation_id, '{}' as group_name, groupArraySample({})(sample) as group_user
                                from (select bitmapToArray(bitmapAndnot((select bitmapBuild(groupArray(id))
                                         from (select distinct id
                                               from ads_user_tag_distribute
                                               where tag_id = {})), (select groupBitmapOrState(gu) as bgu
                                                                     from (select bitmapBuild(group_user) gu
                                                                           from ads_plan_user_group_distribute
                                                                           where operation_id = {})
                                                                     group by {}))) as uid)
                                                                     array join uid as sample;
            """.format(operationid,columns[i],math.ceil(tag_id_num*percent[i]),targetid,operationid,operationid)
            print(sql)
            name['part'+str(i)] = sql
            randomInsert(n,list_part,list_all,num,name['part'+str(i)])
            group_number.append(0)
            
    return group_number,columns


# In[171]:


group_number,list_group = get_group(4,operation_id,targetUserId,dict)


# # 插入数据进相应的ads_operation_detail表

# In[172]:


import psycopg2
 
#连接数据库
conn = psycopg2.connect(database = 'ftabcdb', user = 'ftabc', password = 'aihub@2020', host = 'ftabc-postgresql.default.svc.cluster.local', port = '5432')
 
curs=conn.cursor()


# In[173]:

import datetime
import numpy as np
insert_date = str(datetime.date.today())

for i in range(len(group_number)):
    #编写Sql
    #插入数据
    insert = """insert into ads_operation_detail (date,goal,goal_name,target_completion_rate,p_customer_num,p_plan_trigger_num,target_completion_num,"group",operation_id) values ('{}','{}','{}','{}','{}','{}','{}','{}','{}')"""
    insert_sql = insert.format(insert_date,'首要目标','启动','0.0',group_number[i],group_number[i],0,list_group[i],operation_id)
    #数据库中执行sql命令
    curs.execute(insert_sql)
    #提交数据    
    conn.commit()

#关闭指针和数据库
curs.close()
conn.close()
# In[ ]:




