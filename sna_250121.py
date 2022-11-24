# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import numpy as np
from graph_tool.all import *
import matplotlib.pyplot as plt
import time
from gi.repository import Gtk, Gdk, GdkPixbuf, GObject, GLib

#annoying global variables
i=0
dont_redraw=0
lasttimerun=0

# %%
plt.switch_backend("GTK3Cairo")
df=pd.read_csv('/data/network/20210111_Monash_SNA_Data.csv')
#put code in here to sort by timestamp (sample file already is)
df.sort_values(by=['Timestamp'],inplace=True)
#print(df)


# %%
senders=df[["Sender"]]
receivers=df[["Receiver"]]


# %%
all_ids=np.concatenate([senders.to_numpy(),receivers.to_numpy()])


# %%
all_unique_ids=np.unique(all_ids)


# %%
#stats
num_messages=len(df.index)
num_users=all_unique_ids.size

print("Total number of messages (edges): ",num_messages)
print("Total number of unique users (nodes): ",num_users)
# earliest_timestamp=


# %%
# Graph variables 
g=Graph()
v_sender_id=g.new_vertex_property("int")
v_receiver_id=g.new_vertex_property("int")
e_timestamp=g.new_edge_property("int")
e_msg_type=g.new_edge_property("string")
e_msg_num=g.new_edge_property("int")
e_num_messages=g.new_edge_property("int") # use to weight edges based on num of messages between two given nodes 

senders=[]
receivers=[]
senders_set=set()
receivers_set=set()
sender_map={} # python dict for faster match of vertexid to senderid. Key=sender_id
receiver_map={} # python dict for faster match of vertexid to receiverid. Key=receiver_id
timestamp_map={} # python dict mapping edges to timestamps. Key=edge_id
max_nodes=10000
num_edges=0
num_vertices=0


# %%
for row in df.itertuples():
    index=row[0]
    sender_id=row[1]
    msg_num=row[2]
    msg_type=row[3]
    timestamp=row[4]
    receiver_id=row[5]
    #print(receiver_id)
    #if index % 100 ==0:
    #    print(index)
    if index == max_nodes:
        break
    
     #sender_exists=0
     #receiver_exists=0
     
    #print (senders)
    #print ("Sender=",sender," Receiver=",receiver)
    
 
    # look to see if sender exists in network
    if sender_id not in senders_set:
        #print("adding",sender_id)
        #senders.append(sender_id)
        senders_set.add(sender_id)
        sender_v=g.add_vertex()
        #v_sender_id[sender_v]=sender_id
        sender_map[sender_id]=g.vertex_index[sender_v]
    else:
        #sender_exists=1
        #v_sender_id_array=v_sender_id.get_array()
        #print("array is",v_sender_id.get_array())
        existing_sender_index=sender_map[sender_id]
        #existing_sender_index=list(v_sender_id_array).index(sender_id)
        sender_v=g.vertex(existing_sender_index)
        #print (existing_vertex_index)
        #print("not adding",sender_id)
        
    # look to see if receiver exists in network
    if receiver_id not in receivers_set:
        #receivers.append(receiver_id)
        receivers_set.add(receiver_id)
        receiver_v=g.add_vertex()
        #v_receiver_id[receiver_v]=receiver_id
        receiver_map[receiver_id]=g.vertex_index[receiver_v]
    else:
        #print("Receiver exists")
        #receiver_exists=1
        #v_receiver_id_array=v_receiver_id.get_array()
        existing_receiver_index=receiver_map[receiver_id]
        #existing_receiver_index=list(v_receiver_id_array).index(receiver_id)
        receiver_v=g.vertex(existing_receiver_index) 
 
    #if sender_exists==1 and receiver_exists==1:
       # print("both sender and receiver exist")
   
    edge=g.add_edge(sender_v,receiver_v)
    num_edges=num_edges+1
    edge_id=g.edge_index[edge]
    timestamp_map[edge_id]=timestamp
    
print("Graph built with",max_nodes," nodes. There are",num_edges," edges.")
deg_in = g.degree_property_map("in")
deg_out = g.degree_property_map("out")



# %%
# draw the graph as an inline window in notebook
def draw_graph_inline(g):
    graph_draw(g, inline=True, vertex_size=2)

def draw_graph_window(g):
    win=GraphWindow(g,pos=sfdp_layout(g,K=0.5),geometry=(500,400))
    win.graph.regenerate_surface()
    win.graph.queue_draw()
    win.show_all()
    Gtk.main()

# %%
def filter_graph_timestamps(g,min_timestamp,max_timestamp):
    # keep only those edges and vertices in graph between min and max timestamp inclusive
    # create relevant property map for the edges involved
    edge_filter=g.new_edge_property("bool",val=True)
    vertex_filter=g.new_vertex_property("bool",val=True)
    
    # now populate the maps
    # 1. set relevant edges to be filtered - is this an efficient way to do this (better iterate across timestamps - but have to lookup edge ids) 
    for e in g.edges():
        edge_id=g.edge_index[e]
        edge_timestamp=timestamp_map[edge_id]
        #print(edge_id,edge_timestamp)
        if edge_timestamp >= min_timestamp and edge_timestamp <=max_timestamp:
            edge_filter[e]=False
            #print(edge_id,edge_timestamp)
   
    # 2. filter out all vertices that have in degree and out degree zero
    for v in g.vertices():
        if v.in_degree()==0 and v.out_degree()==0:
                print(v) 
                vertex_filter[v]=False
        
    
    g.set_edge_filter(edge_filter)
    g.set_vertex_filter(vertex_filter)
        

def new_successive_build(g,start_timestamp,end_timestamp,update_rate,update_interval):
    #update_rate = time between redraws (milliseconds)
    #update_interval = timestamp step between regeneration of filter (integral number of seconds)
    # will only build forward in time (start_timestamp is fixed)
    # set initial filter 
    start=start_timestamp
    end=end_timestamp
    step=0.5 # parameter to be tuned in interface
    K=0.1 #parameter to be tuned in interface
    
    pos=sfdp_layout(g,K=K)
    win=GraphWindow(g,pos,geometry=(2000,2000))
    lasttimerun=int(round(time.time()*1000))

    def update_state():
      global i
      global lasttimerun
      timenow=int(round(time.time()*1000))

      if timenow-lasttimerun > update_interval:
        end_t=end
        start_t=start
        end_draw=start_t+i*update_interval
        if end_draw <= end_t:
            filter_graph_timestamps(g,start_t,end_draw)
            sfdp_layout(g,pos=pos,K=K,init_step=step,max_iter=1)
        win.graph.regenerate_surface()
        win.graph.queue_draw()
        print("Debug: start_timestamp=", start_t, ": end_timestamp=", end_draw)
        i=i+1
        if i>0 and i%50 == 0:
              win.graph.fit_to_window(ink=True)
        lasttimerun=int(round(time.time()*1000))
      return True
     
    cid=GLib.idle_add(update_state)
    win.connect("delete_event",Gtk.main_quit)
    win.show_all()
    Gtk.main()    


   

# %%
def simple_successive_build(df,start_timestamp,end_timestamp,update_interval):
    num_edges_anim=0
    num_vertices_anim=0
    g_anim=Graph()
    win=None
    senders_anim=[]
    receivers_anim=[]
    senders_set_anim=set()
    receivers_set_anim=set()
    sender_map_anim={} # python dict for faster match of vertexid to senderid. Key=sender_id
    receiver_map_anim={} # python dict for faster match of vertexid to receiverid. Key=receiver_id
    timestamp_map_anim={} # python dict mapping edges to timestamps. Key=edge_id 
    pos=sfdp_layout(g_anim, K=0.5)
    g_window=GraphWindow(g_anim,pos,geometry=(1000,800))
    
    #pos=g_anim.new_vertex_property("vector<double>")
    #print(pos.a)
    df_subset=df.loc[(df['Timestamp']>=start_timestamp) & (df['Timestamp']<=end_timestamp)]
    tuple_iterator=df_subset.itertuples()
    print("Time window contains",len(df_subset)," messages (edges)")
    #gw=GraphWidget(g_anim,pos)
    win=GraphWindow(g_anim,pos,geometry=(1000,800),vertex_text=g_anim.vertex_index,update_layout=True)
    #g_anim.add_vertex()
    #g_anim.add_vertex()
    # g_anim.add_edge(0,1)
 
    for row in tuple_iterator:
        index=row[0]
        sender_id=row[1]
        msg_num=row[2]
        msg_type=row[3]
        timestamp=row[4]
        receiver_id=row[5]
        #print(row)    
        if sender_id not in senders_set_anim:
            senders_set_anim.add(sender_id)
            sender_v=g_anim.add_vertex()
            sender_map_anim[sender_id]=g_anim.vertex_index[sender_v]
        else:
            existing_sender_index=sender_map_anim[sender_id]
            sender_v=g_anim.vertex(existing_sender_index)
                
        if receiver_id not in receivers_set_anim:
            receivers_set_anim.add(receiver_id)
            receiver_v=g_anim.add_vertex()
            receiver_map_anim[receiver_id]=g_anim.vertex_index[receiver_v]
        else:
            existing_receiver_index=receiver_map_anim[receiver_id]
            receiver_v=g_anim.vertex(existing_receiver_index) 
 
        print("Adding edge between ",g_anim.vertex_index[sender_v]," and ",g_anim.vertex_index[receiver_v])
        edge=g_anim.add_edge(sender_v,receiver_v)
        #num_edges_anim=num_edges_anim+1
        edge_id=g_anim.edge_index[edge]  
        print("edge_id=",edge_id)
        timestamp_map_anim[edge_id]=timestamp
        #random_layout(g_anim,pos=pos)
        pos=sfdp_layout(g_anim,K=0.5)
        g_window.pos=pos
        #win=graph_draw(g_anim,pos)
    print("done with simple_successive_build")





def successive_build(df,start_timestamp,end_timestamp,update_interval):
   

    #extract relevant rows from full dataframe
    #update_interval is in seconds (can be fractional)
    
    num_edges_anim=0
    num_vertices_anim=0
    g_anim=Graph()
    #g_anim.add_vertex()
    senders_anim=[]
    receivers_anim=[]
    senders_set_anim=set()
    receivers_set_anim=set()
    sender_map_anim={} # python dict for faster match of vertexid to senderid. Key=sender_id
    receiver_map_anim={} # python dict for faster match of vertexid to receiverid. Key=receiver_id
    timestamp_map_anim={} # python dict mapping edges to timestamps. Key=edge_id 
    pos=sfdp_layout(g_anim, K=0.5)
  
    df_subset=df.loc[(df['Timestamp']>=start_timestamp) & (df['Timestamp']<=end_timestamp)]
    tuple_iterator=df_subset.itertuples()
    print("Time window contains",len(df_subset)," messages (edges)")
    win=GraphWindow(g_anim,pos=pos,geometry=(500,400))
    more_to_go=1 #flag indicating more to read from iterator
    
   
    def update_state():
        pos=sfdp_layout(g_anim,K=0.5)
        print("Started update_state")
        # have to add timing code to implement update interval - currently updates as rapidly as possible (each call)
       
        try:
            row=next(tuple_iterator)
            more_to_go=1
        except StopIteration as stoperror:
            more_to_go=0
            #print("All done with tuples")
        
        if (more_to_go==1):
            index=row[0]
            sender_id=row[1]
            msg_num=row[2]
            msg_type=row[3]
            timestamp=row[4]
            receiver_id=row[5]
            if sender_id not in senders_set_anim:
                senders_set_anim.add(sender_id)
                sender_v=g_anim.add_vertex()
                sender_map_anim[sender_id]=g_anim.vertex_index[sender_v]
            else:
                existing_sender_index=sender_map_anim[sender_id]
                sender_v=g_anim.vertex(existing_sender_index)
                
            if receiver_id not in receivers_set_anim:
                receivers_set_anim.add(receiver_id)
                receiver_v=g_anim.add_vertex()
                receiver_map_anim[receiver_id]=g_anim.vertex_index[receiver_v]
            else:
                existing_receiver_index=receiver_map_anim[receiver_id]
                receiver_v=g_anim.vertex(existing_receiver_index) 
 
            print("Adding edge between ",g_anim.vertex_index[sender_v]," and ",g_anim.vertex_index[receiver_v])
            edge=g_anim.add_edge(sender_v,receiver_v)
            #num_edges_anim=num_edges_anim+1
            edge_id=g_anim.edge_index[edge]  
            print("edge_id=",edge_id)
            timestamp_map_anim[edge_id]=timestamp
            #random_layout(g_anim,pos=pos)
    
        #pos=sfdp_layout(g_anim)
        print("Running sfdp_layout")
        newpos=sfdp_layout(g_anim,K=1,max_iter=1)
        pos_array=newpos.get_array()
        win.pos=newpos
        print("newpos array = ",pos_array)
        print("here")
        print("There are",len(g_anim.get_vertices())," vertices in the graph now.")
        print("There are",len(g_anim.get_edges())," edges in the graph now.")
        win.graph.regenerate_surface()
        win.graph.queue_draw()
        sleep(5)
        #print("calling update_state()")
        print("Number of vertices in graph on update_state=",g_anim.num_vertices())
        return True
        
    
    cid=GLib.idle_add(update_state)
    win.connect("delete_event",Gtk.main_quit)
    win.show_all()
    Gtk.main()

        
        
    
        
    
    
    
    print("Graph built. There are",num_edges," edges.")
    #deg_in = g_anim.degree_property_map("in")
    #deg_out = g_anim.degree_property_map("out")
    


# %%



# %%
#interactive
#pos=sfdp_layout(g,K=0.5)
#window=GraphWindow(g,pos,geometry=(500,500))


# %%
#draw_graph_window(g)
#filter_graph_timestamps(g,1575939024,1590707115)
#draw_graph_window(g)
new_successive_build(g,1573368466,1590317423,0.5,5000)
#successive_build(df,1573368466,1590317423,0.5)

### 
# %%



# %%



