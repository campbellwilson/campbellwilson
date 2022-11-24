# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import numpy as np
from graph_tool.all import *
import matplotlib.pyplot as plt
import time
from gi.repository import Gtk, Gdk, GdkPixbuf, GObject, GLib, Pango
from datetime import datetime

#annoying global variables
i=0
dont_redraw=0
lasttimerun=0
progress_percent=0
ageing_rate=5
# %%
plt.switch_backend("GTK3Cairo")
df=pd.read_csv('/data/network/20210111_Monash_SNA_Data.csv')
#put code in here to sort by timestamp (sample file already is)
df.sort_values(by=['Timestamp'],inplace=True)
#print(df)


# %%
senders=df[["Sender"]]
receivers=df[["Receiver"]]
analysing=0 # global set when analysing the graph (also suspends animation)

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


step=0.005 # parameter to be tuned in interface
K=0.5 #parameter to be tuned in interface
pos=sfdp_layout(g,K=K)

edge_filter=g.new_edge_property("bool",val=False)
vertex_filter=g.new_vertex_property("bool",val=False)
v_sender_id=g.new_vertex_property("int")
v_receiver_id=g.new_vertex_property("int")
e_timestamp=g.new_edge_property("int")
e_msg_type=g.new_edge_property("string")
e_msg_num=g.new_edge_property("int")
e_num_messages=g.new_edge_property("int") # use to weight edges based on num of messages between two given nodes 

red_blue_map={0:(1,0,0,1),1:(0,0,1,1)}
vertex_colors=g.new_vertex_property("vector<double>")
edge_colors=g.new_edge_property("vector<double>")
vertex_sizes=g.new_vertex_property("int",10)
g.vertex_properties['vertex_colors']=vertex_colors
g.vertex_properties['vertex_sizes']=vertex_sizes
senders=[]
receivers=[]
edge_by_time=[] # will be an array of tuples <timestamp,edge_id>.  Because datafile is sorted by time stamp, edge_id will increase with time
senders_set=set()
receivers_set=set()
sender_map={} # python dict for faster match of vertexid to senderid. Key=sender_id
receiver_map={} # python dict for faster match of vertexid to receiverid. Key=receiver_id
timestamp_map={} # python dict mapping edges to timestamps. Key=edge_id
max_nodes=10000 #how many nodes in the whole graph 
num_edges=0
num_vertices=0
suspend_draw=0 #flag for stopping and starting graph redraw
edge_id_tracker=0 # will be used to keep track of edges added to the edge_filter
edge_timestamp={} # python dict key=timestamp+edge_id (should be unique), value = edge descriptor
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
    # need a way to lookup a set of edge descriptors given a timestamp, and order by timestamp. It is possible for there to be more than one edge for the same timestamp (more than one message per second - unlikely but possible)
    timestamped_edge_string=str(timestamp)+str(edge_id) # do this to ensure a unique key in the following dictionary
    #edge_by_time.append([timestamp,edge,edge_id]) #this array should be ordered by timestamp
    edge_timestamp[timestamped_edge_string]=edge

    
    
print("Graph built with",max_nodes," nodes. There are",num_edges," edges.")
print("-----DEBUG --- Dump edge_by_time array")
print(edge_by_time)
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
def filter_graph_timestamps(g,min_timestamp,max_timestamp,end_timestamp):
    # keep only those edges and vertices in graph between min and max timestamp inclusive
    # create relevant property map for the edges involved
    #edge_filter=g.new_edge_property("bool",val=False)
    #vertex_filter=g.new_vertex_property("bool",val=False)
    #end_timestamp is only used for determining overall age of message across all epochs of drawing
    # now populate the maps
    # 1. set relevant edges to be filtered - is this an efficient way to do this (better iterate across timestamps - but have to lookup edge ids) 
    # note: edge_index from graph-tool only guaranteed to be in range 0, num-edges-1 if no edges have been deleted
    size_v=0
    size_e=0
    g.clear_filters()
    for e in g.edges():
        edge_id=g.edge_index[e]
        source_v=e.source()
        target_v=e.target()
        edge_timestamp=timestamp_map[edge_id]
        #vertex_colors[target_v]=[0.2,1.0,0.0,1.0]
     #   print("DEBUG:",edge_id,edge_timestamp)
        if edge_timestamp >= min_timestamp and edge_timestamp <= max_timestamp:
            edge_age=max_timestamp-edge_timestamp
            edge_age_norm=edge_age/(end_timestamp-min_timestamp+1)
            #print("edge_age_norm=",edge_age_norm)
            #color RGBA tuple with all values in [0,1]
            edge_r=edge_age_norm*ageing_rate
            vertex_colors[source_v]=[1-edge_r,0.3,0.0,1.0-edge_r]
            vertex_colors[target_v]=[1-edge_r,0.3,0.0,1.0-edge_r]
            edge_colors[e]=[1-edge_r,0.2,0.1,1.0-edge_r*.5]
            edge_filter[e]=True
            vertex_filter[source_v]=True
            vertex_filter[target_v]=True
            vertex_age_size=14-5*edge_age_norm*ageing_rate*0.8
            #print("vertex_age_size=",vertex_age_size)
            vertex_sizes[source_v]=vertex_age_size
            vertex_sizes[target_v]=vertex_age_size
            size_v=size_v+2
            size_e=size_e+1
            
            #print(edge_id,edge_timestamp)
   
    # 2. filter out all vertices that have in degree and out degree zero
    #for v in g.vertices():
     #   if v.in_degree()==0 and v.out_degree()==0:
      #          print(v) 
       #         vertex_filter[v]=False
        
    #print("Should be displaying: ",size_v, " vertices and",size_e," edges!")
    g.set_edge_filter(edge_filter)
    g.set_vertex_filter(vertex_filter)
        
# AiLECS-ify the GraphWindow class
#class AiLECS_GraphWindow(GraphWindow):
#    def __init__(self):
#        GraphWindow.__init__(self,g,pos,geometry=(3000,2000))
#        self.set_title("AiLECS/Anonymised Text Messaging Network Demo")
#        self.vertex_size=10
#        # name of the widget containing the graph is "graph"
#        self.graph.vprops={"size":10}
#        self.graph.eprops={"pen_width":5}

def update_edge_filter(timestamped_edges):
    # add edges with list of timestamped edges to the edge filter
    for te in timestamped_edges:
        e=edge_timestamp[te]
        edge_filter[e]=True
        vertex_filter[e.source()]=True
        vertex_filter[e.target()]=True


class AiLECS_GraphWindow(Gtk.Window):
    def __init__(self, g, pos, geometry, vprops=None, bg_color="[0.7,0.1,0.1,1.0]",max_render_time=10,fit_view_ink=True, eprops=None, vorder=None, eorder=None, nodesfirst=False, update_layout=False, **kwargs):
        Gtk.Window.__init__(self, title="AiLECS")          
        
        self.box1=Gtk.Box(orientation=0,spacing=6)
        self.box2=Gtk.Box(orientation=1,spacing=6)
        self.transport_box=Gtk.Box(orientation=0,spacing=6)
        self.info_box=Gtk.Box(orientation=1,spacing=6)
        self.play_button=Gtk.Button(label="Play")
        self.stop_button=Gtk.Button(label="Pause")
        self.analyse_button=Gtk.Button(label="Analyse")
        self.progress_bar=Gtk.ProgressBar()
        
        self.play_button.connect("clicked",self.on_play_clicked)
        self.stop_button.connect("clicked",self.on_stop_clicked)
        self.analyse_button.connect("clicked",self.on_analyse_clicked)
        
        self.add(self.box1)
        self.add(self.box2)
        #self.add(self.play_button)
        #self.add(self.stop_button)
        #self.add(self.progress_bar)

        self.set_default_size(geometry[0], geometry[1])
        self.graph = GraphWidget(g, pos, vprops, eprops, vorder, eorder, nodesfirst, update_layout, **kwargs)
        #self.add(self.graph)
        self.timestamp = Gtk.Label("timestamp")
        self.timestamp.modify_font(Pango.FontDescription('18'))
        
        self.status_text=Gtk.Label("status_text")
              
        #self.timestamp.set_size_request(-1,100)')
        self.set_vexpand(False)
        self.set_hexpand(False)
        #self.timestamp.connect("clicked", self.on_button_clicked)
        self.transport_box.pack_start(self.play_button,False,True,0)
        self.transport_box.pack_start(self.stop_button,False,True,0)
        self.transport_box.pack_start(self.analyse_button,False,True,0)
        self.info_box.pack_start(self.timestamp,False,True,0)
        self.info_box.pack_start(self.progress_bar,False,True,0)
        self.info_box.pack_start(self.status_text,False,True,0)
        self.status_text.set_text("Initialising")
        self.box2.pack_start(self.transport_box,False,True,0)
        self.box2.pack_start(self.info_box,False,True,0)
        self.box1.pack_start(self.graph,True,True,0)
        self.box1.pack_start(self.box2,False,True,0)

        self.timeout_id=GLib.timeout_add(50,self.on_timeout,None)
    
    def on_play_clicked(self, play_button):
        # start graph drawing
        global suspend_draw
        suspend_draw=0
        
        
    def on_stop_clicked(self,stop_button):
        # suspend graph drawing
        global suspend_draw
        suspend_draw=1
        
    def on_timeout(self, user_data):
        self.progress_bar.set_fraction(float(progress_percent)/100.0)
        return True
    
    def on_analyse_clicked(self,user_data):
        analyse(self)
        return True

def analyse(win):
    global suspend_draw,g,analysing
    #suspend_draw=1
    analysing=1
    print("I am in analyse class now")
    win.status_text.set_text("Calculating PageRank")
    pr_map=pagerank(g)
    # normalise pagerank values to be between 0 and 1
    max_pr=0
    for v in g.vertices():
        print(pr_map[v])
        if pr_map[v]>max_pr:
            max_pr=pr_map[v]
    # recolor according to normalised page_rank
    for v in g.vertices():
        pr_map[v]=pr_map[v]/max_pr    
        print(pr_map[v])
        vertex_colors[v]=[1-pr_map[v],1-pr_map[v],1-pr_map[v],1.0]

    # redraw
    win.graph.regenerate_surface()
    win.graph.queue_draw()
    suspend_draw=1
    analysing=0
   
   # now recolor graph according to page rank
   




def new_successive_build(start_timestamp,end_timestamp,update_rate,update_interval):
    #update_rate = time between redraws (milliseconds)
    #update_interval = timestamp step between regeneration of filter (integral number of seconds)
    # will only build forward in time (start_timestamp is fixed)
    # set initial filter 
    
    global g
    start=start_timestamp
    end=end_timestamp
   
    
    local_pos=sfdp_layout(g,K=0.5)
    
    #win=AiLECS_GraphWindow()
    #win.graph.pos=local_pos
    win=AiLECS_GraphWindow(g,local_pos,geometry=(1500,1000),vertex_size=vertex_sizes,edge_pen_width=1.2,vertex_fill_color=vertex_colors,edge_color=edge_colors)
    
    

    lasttimerun=int(round(time.time()*1000))

    def update_state():
      global i, lasttimerun, progress_percent
      timenow=int(round(time.time()*1000))
      

      if timenow-lasttimerun > update_rate and suspend_draw==0 and analysing==0:
        end_t=end
        start_t=start
        end_draw=start_t+i*update_interval
        progress_percent=int((end_draw-start_t)/(end_t-start_t)*100)
        
        print("percent=",progress_percent)
        if end_draw <= end_t:
            
            
            
            
            
            
            filter_graph_timestamps(g,start_t,end_draw,end_timestamp)
            sfdp_layout(g,pos=local_pos,K=K,init_step=step,max_iter=3)            
            win.graph.regenerate_surface()
            win.graph.queue_draw()
            print("Debug: start_timestamp=", start_t, ": end_timestamp=", end_draw)
            i=i+1
            if i>0 and i%1000 == 0:
                win.graph.fit_to_window(ink=True)
            lasttimerun=int(round(time.time()*1000))
        print("timenow=",timenow, "lasttimerun=",lasttimerun)
        win.timestamp.set_text(str(datetime.fromtimestamp(end_draw)))
        win.status_text.set_text("Animating network")
      elif suspend_draw==1:
        win.status_text.set_text("Animating network (Paused)")
        #win.progress_bar.set_fraction(float(100.0/float(progress_percent)))
      return True
     
    cid=GLib.idle_add(update_state)
    win.connect("delete_event",Gtk.main_quit)
    win.show_all()
    Gtk.main()    


   

# %%

# %%










new_successive_build(1590705805,1590738058,10,40)
#new_successive_build(1590737058,1590938058,500,400)

