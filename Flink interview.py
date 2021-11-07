#!/usr/bin/env python
# coding: utf-8

# In[2]:


import json
import pandas as pd
import pandera as pa
from validate_email import validate_email
from sqlalchemy import create_engine


# In[ ]:


# Reading data and writing to a Pandas Dataframe

data = [json.loads(line) for line in open('202110_flink_data_engieering_sample_data (1).json', 'r')]
df = pd.DataFrame(data)
df[['user_email','phone_number']] = pd.DataFrame(df.data.tolist(), index= df.index)
df = df.drop(columns = ['data'])

df.info(verbose=True)


# In[ ]:


# Schema Validation

df = df.astype({'user_email': str})
df['event_time'] = pd.to_datetime(df['event_time'], format="%Y-%m-%d %H:%M:%S")
df['processing_date'] = pd.to_datetime(df['processing_date'], format="%Y-%m-%d")
df = df['user_email'].apply(validate_email)

df.count()


# In[ ]:


# Saving to the DB

engine = create_engine('')
df.to_sql('public.flink', engine)


# In[ ]:


# Reading from Database

db_df = pd.read_sql_query('SELECT * FROM public.flink',con=engine)


# In[3]:


# Check difference between 2 dataframe (Local and from DB)

comparison = pd.concat([db_df, df])
comparison = comparison.reset_index(drop=True)
df_gpby = df.groupby(list(df.columns))
idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]


# In[ ]:




