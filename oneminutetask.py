#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#One minute Task

#import required modules
import json
import time
from sseclient import SSEClient as EventSource
from collections import defaultdict
url = 'https://stream.wikimedia.org/v2/stream/revision-create'
#function to generate a list of domain information and a list of user information every minute from a continuous streaming API
def one_min():
    lst=[]
    info_domain=defaultdict(list)
    info_user=defaultdict(list)
    domain={}
    user={}
    a=time.time() #initialize the start time in seconds
    for event in EventSource(url):
        if (time.time()<=a+60): #condition to load the data for a minute
                try:
                    change = json.loads(event.data)
                except ValueError:
                        continue
                else:
                    info_domain[change['meta']['domain']].append(change['page_title']) #store the domain name as key and page titles edited as values in a dictionary
                    if change['meta']['domain']=='en.wikipedia.org' and change['performer']['user_is_bot']==False:#Condition to check the domain name and whether the user is a bot or not
                        if 'user_edit_count' in change['performer'].keys():#condition to check if the user_edit_count data is present in the event
                            info_user[change['performer']['user_text']].append(change['performer']['user_edit_count'])
                        else:
                            info_user[change['performer']['user_text']].append(0)#using zero as user_edit_count where the user_edit_count data is not present in the event
        else:
            break
    #make a dictionary with keys as domain names and values as number of distinct pages edited
    for key in info_domain.keys():
        domain[key]=len(set(info_domain[key]))
    #sort the dictionary in descending order
    domain=sorted(domain.items(), key = 
             lambda kv:(kv[1], kv[0]),reverse=True)
    #make a dictionary with keys as user name and values as the maximum of user count
    for key in info_user.keys():
        user[key]=max(info_user[key])
    #sort the dictionary in descending order
    user=sorted(user.items(), key = 
             lambda kv:(kv[1], kv[0]),reverse=True)
        
    lst.append(domain)   
    lst.append(user)
    return lst

#function for printing out the reports
def print_rep():
        f=one_min()#call generate function
        d=f[0]
        s=len(d)
        print("\n DOMAIN REPORT \n")
        print("Total numbers of Domains Updated:"+str(s)+"\n")
        for el in d:
            print(el[0]+": "+str(el[1])+" pages updated")
        du=f[1]
        #check the condition whether the number of users editing is zero or not
        if len(du) !=0:
            print("\n USERS REPORT \n")
            for elu in du:
                    print(str(elu[0]) +': '+ str(elu[1]))
        else:
            print("\n USERS REPORT \n")
            print("NONE:NONE")

        
#Driver function:Call the print function infinitely.
while True:
    print_rep()
 


# In[ ]:




