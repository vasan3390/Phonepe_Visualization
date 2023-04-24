#!/usr/bin/env python
# coding: utf-8

# In[306]:


import pandas as pd
import json 
import plotly.express as px
from sqlalchemy import create_engine
from pandas.io import sql
import pymysql


# In[307]:


with open(r'C:\Users\Lenovo\Downloads\pulse-master\pulse-master\data\map\transaction\hover\country\india\2022\4.json','r') as f:
    datas = json.loads(f.read()),
df_nested_list = pd.json_normalize(datas,record_path =['data','hoverDataList','metric'],meta=['success','code',['data','hoverDataList','name']]) 
df_nested_list


# In[308]:


def cap_sentence(s):
  return ' '.join(w[:1].upper() + w[1:] for w in s.split(' '))


# In[309]:


bcd = df_nested_list["data.hoverDataList.name"].str.replace(r'(\w+)', lambda x: x.group().capitalize(),n=2, regex=True)


# In[310]:


bcd


# In[311]:


df_nested_list['state']=bcd


# In[312]:


df_nested_list


# In[ ]:





# In[313]:


df_nested_list.drop(['data.hoverDataList.name'], axis=1,inplace=True)

# df.drop(df.columns[[0, 4, 2]], axis=1, inplace=True)


# In[314]:


df_nested_list


# In[315]:


engine = create_engine("mysql+pymysql://root:Vasan3390@localhost:3306/vasan",pool_size=1000, max_overflow=2000)


# In[316]:


df_nested_list.to_sql('trans_2022_4', engine, if_exists='append', index=False, chunksize=None, dtype=None, method=None)


# In[317]:


connection = pymysql.connect(host='localhost',
                             user='root',
                             password='Vasan3390',
                             db='vasan')


# In[318]:


cursor = connection.cursor()


# In[319]:


sql='select * from trans_2022_4'


# In[ ]:





# In[320]:


mysql_df=pd.read_sql(sql, engine, index_col=None,chunksize=None)


# In[321]:


mysql_df


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[322]:


fig = px.choropleth(
  mysql_df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='state',
    color='count',
    color_continuous_scale='blues'
)

fig.update_geos(fitbounds="locations", visible=False)

fig.show()


# In[ ]:




