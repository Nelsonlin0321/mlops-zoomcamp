#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip freeze | grep scikit-learn')


# In[2]:


import pickle
import pandas as pd
import numpy as np


# In[3]:


with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


# In[4]:


categorical = ['PULocationID', 'DOLocationID']

def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


# In[5]:


year = 2022
month = 2


# In[6]:


df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet')


# In[7]:


dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)


# In[8]:


np.std(y_pred)


# In[9]:


df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


# In[10]:


df['predicted_duration']=y_pred


# In[11]:


df_output = df[['ride_id','predicted_duration']]


# In[12]:


df_output.head()


# In[13]:


df_output.to_parquet(
    f"yellow_tripdata_{year:04d}-{month:02d}_duration_prediction.parquet",
    engine='pyarrow',
    compression=None,
    index=False
)


# In[16]:


df = pd.read_parquet("yellow_tripdata_2022-03_duration_prediction.parquet")


# In[17]:


df.head()


# In[18]:


np.mean(df['predicted_duration'])


# In[ ]:




