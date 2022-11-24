#!/usr/bin/env python
# coding: utf-8
# changed 24/11/22
# In[1]:

# i changes something in this branch`
import pandas as pd
import numpy as np
from graph_tool.all import *
import matplotlib.pyplot as plt

# but this time i made a different change
# In[2]:


plt.switch_backend("GTK3Cairo")
df=pd.read_csv('/data/network/20210111_Monash_SNA_Data.csv')
#put code in here to sort by timestamp (sample file already is)
print(df)


# In[3]:


senders=df[["Sender"]]
receivers=df[["Receiver"]]


# In[4]:


all_ids=np.concatenate([senders.to_numpy(),receivers.to_numpy()])


# In[5]:


all_unique_ids=np.unique(all_ids)


# In[6]:


#stats
num_messages=len(df.index)
num_users=all_unique_ids.size

print("Total number of messages (edges): ",num_messages)
print("Total number of unique users (nodes): ",num_users)


# In[7]:


g=Graph()
e_timestamp=g.new_edge_property("int")
e_msg_type=g.new_edge_property("string")
e_msg_num=g.new_edge_property("int")


# In[ ]:


for row in df.itertuples():
    index=row[0]
    sender=row[1]
    msg_num=row[2]
    msg_type=row[3]
    timestamp=row[4]
    receiver=row[5]
    if index == 2:
        break
    print ("Sender=",sender," Receiver=",receiver)
    sender_v=g.add_vertex()
    receiver_v=g.add_vertex()
    msg_edge=g.add_edge(sender,receiver)
    
interactive_window(g)
    #graph_draw(g,vertex_text=g.vertex_index, output="graph.pdf")


# In[ ]:





# In[ ]:




