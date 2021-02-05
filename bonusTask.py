#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#BONUS TASK

#import required modules

import json
import time
from sseclient import SSEClient as EventSource
from collections import defaultdict
from collections import Counter

url = 'https://stream.wikimedia.org/v2/stream/revision-create'

#list for storing domain information
lst_domain=[]
#list for storing user information
lst_user=[]

#function to add values of common keys from a list of dictionaries
def add_dic_values(lst):
    n=len(lst)
    lst1=[]
    for i in range(n):
        lst1.append(Counter(lst[i]))
    counter=lst1[0]
    for j in range(1,n):
        counter+=lst1[j]
    return counter

#function to sort dictionary by values in descending order
def sort_dic(dic):
    dic=sorted(dic.items(), key = 
             lambda kv:(kv[1], kv[0]),reverse=True)
    return dic

#function to find maximum of values of common keys from a list of dictionaries
def max_dict(lst):
    
    def none_max(a, b):
        if a is None:
            return b
        if b is None:
            return a
        return max(a, b)

    n=len(lst)
    if n==1:
        return lst[0]
    elif n==2:
        all_keys = lst[0].keys() | lst[1].keys()
        return  {k: none_max(lst[0].get(k), lst[1].get(k)) for k in all_keys}
    elif n>2:
        while(n > 1):
            all_keys = lst[0].keys() | lst[1].keys()
            a={k: none_max(lst[0].get(k), lst[1].get(k)) for k in all_keys}
            lst.pop(0)
            lst.pop(0)
            lst.insert(0,a)
            n=len(lst)
        return lst[0]

#function to generate a list of domain information and a list of user information every minute from a continuous streaming API
def generate():
    info_domain=defaultdict(list) 
    info_user=defaultdict(list)
    domain={}
    user={}
    start_time=time.time() #initialize the start time in seconds
    for event in EventSource(url):
        if (time.time()<=start_time+5): #condition to load the data for a minute
                try:
                    change = json.loads(event.data)
                except ValueError:
                        continue
                else:
                    info_domain[change['meta']['domain']].append(change['page_title'])#store the domain name as key and page titles edited as values in a dictionary
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
    sort_dic(domain)
    #make a dictionary with keys as user name and values as the maximum of user count
    for key in info_user.keys():
        user[key]=max(info_user[key])
    #sort the dictionary in descending order
    sort_dic(user)
    
    #store domain information in a lsit
    lst_domain.append(domain)
    #make a FIFO structure, such that only past five minute domain values are stored
    if len(lst_domain)>5:
        lst_domain.pop(0)
    #store user information in a list
    lst_user.append(user)
    #make a FIFO structure, such that only past five minute user values are stored
    if len(lst_user)>5:
        lst_user.pop(0)
    return lst_domain,lst_user

#function for printing out the reports
def print_rep():
        f=generate()#call generate function
        lst_dom=f[0]
        total_dom=add_dic_values(lst_dom)#to get cumulative values generated for domain information
        dom=sort_dic(total_dom)
        d=dom
        s=len(d)
        #print domain report
        print("\n DOMAIN REPORT \n")
        print("Total numbers of Domains Updated:"+str(s)+"\n")
        for el in d:
            print(str(el[0])+": "+str(el[1])+" pages updated")
        lst_use=f[1]
        total_use=max_dict(lst_use)# to get cumulative max values generated for user information
        use=sort_dic(total_use)
        du=use
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




