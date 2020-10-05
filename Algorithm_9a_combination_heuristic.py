# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 09:01:30 2018

@author: 20304269
"""

#source:20190328_greedy_stop_span_assignment_spans_example.py

import pandas as pd
import numpy as np
import time
import csv

picking_lines = []
measurements = []
configurations = []
search_logic_name = []
order1 = []
order2 = []
timing = []

#TODO: input files
file_names = ['20130425_3150_LOLS_2']

for f in file_names:
    
        extension_names = ['_min_span', '_nonidentical_span' , '_nonidentical_min_span', '_span_ratio', '_span_stops', '_non_stops_non_span' , '_ratio_add']
        
        for g in extension_names:   
            
                #INPUT for search logic
                df0 = pd.read_csv(f+'.csv', usecols=[3,6], names = ['BRANCH_NO','LOCATION_CODE'])
                                   
                #INPUT for algorithm
            #   print('name of picking_line:', f+g)
                df = pd.read_csv(f+g+'.csv', header=None)
             
                for col in df:
                    df.loc[col, col] = 100 
                    
                #INPUT for batching + search logic
                df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
                branch_list = df2['BRANCH_NO'].tolist()
                      
                ###different search-logics:
#               ##stops                
                loc = df0.pivot_table(index=['BRANCH_NO'], columns='LOCATION_CODE', aggfunc=lambda x: int(1), fill_value=int(0)) #get locations per order
                loc.loc[:,'STOPS'] = loc.sum(axis=1)
                loc.reset_index(inplace = True)
                stop_sort = loc.sort_values('STOPS')
                o_stops = stop_sort['BRANCH_NO'].tolist()
                o_stops = [float(i) for i in o_stops]
                               
                search_logics = [o_stops]
                search_logic_names = ['orders_stops']
                
                for s, t in zip(search_logics, search_logic_names):
                    
                    search_logics = s
                    search_logic_names = t                                             
                                        
                    #empty lists for batching in algorithm
                    order_1 = []
                    order_2 = []  
                    
                    #batch_list for algorithm                   
                    m = df.values #get a numpy array from df for algorithm
                              
                    ##timing
                    start_time = time.time()
                    
                    ###greedy algorithm   
                    for n in s:
                        for row in m:
                                if row[0] == n and np.isin(n, order_2, invert=True): #choose row according to search-logic AND only choose n's that have not been used in order_2 yet    
                                    order_1.append(n)
                                    choose = min(row)

                                    loc_r = np.where(m[:,0] == n)[0] #row locations in length-1 array
                                    loc_c = np.where(row == choose)[0] #column locations in length-1 array

                                    r = int(loc_r[0]) #!choosing the first entry does not always result in the best result but is fast
                                    c = int(loc_c[0])

                                    n_r = m[r] #row which name will be added to order_1 - similar to n

                                    n_c = m[c]

                                    order_2.append(n_c[0]) #column name that will be added to order_2
                    #                print('element:', m[r][c]) #delete all rows/columns "connected" to this element at once
                                    m = np.delete(m, [r,c], 0) 
                                    m = np.delete(m, [r,c], 1)
                    #                print('new m:', m)  
                                                                    
                    ##timing   
                    elapsed_time_secs = time.time() - start_time           
                   
                    #---------------------------------------------
                    
                    picking_lines.append(f)
                    measurements.append(g)
                    search_logic_name.append(t)
                    order1.append(order_1)
                    order2.append(order_2)
                    timing.append(elapsed_time_secs)              
                                 
                    #---------------------------------------------
                                  
#                    print('order_1: ', order_1)
#                    print('order_2: ', order_2)
#                    print('order_1s length:' , len(order_1))
#                    print('order_2s length:' , len(order_2)) 
                    
                    #---------------------------------------------
                    
                    ###order batching (size of 2)
                    batch_list = []                
                    for y in range(len(branch_list)):
                        for x in range(len(order_1)):
                            if branch_list[y] == order_1[x]:
                                batch_list.append('Batch {}'.format(x+1))
                            elif branch_list[y] == order_2[x]:
                                batch_list.append('Batch {}'.format(x+1))                                 
                    
                    ##Third column with 'BATCH_NO'
                    df2['BATCH_NO'] = batch_list 
    #                print('length of batch_list', len(batch_list))
                      
                    #Group by batch and sum up entries
                    batched = df2.groupby(['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO','BATCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE'], as_index=False).agg \
                     ({'BRANCH_NO' : 'count', \
                      'NUM_UNITS_y' : 'sum', \
                      'NUM_BRANCHES' : 'sum', \
                      'NUM_UNITS_x' : 'sum', \
                      'VOLUME_PER_UNIT' : 'sum',\
                      'WEIGHT_PER_UNIT_KG' : 'sum'})                
                    
                #    #OUTPUT: order_1 and order_2 from greedy algorithm
                #    print('picking line name:', f)
                #    print('order_1:', order_1)
                #    print('order_2:', order_2)
                    
                #    #OUTPUT: batches of size 2
                #    print('picking line name:', f)
                #    print(batched) 
                        
                    batched.to_csv(f+'_batched_2'+g+'_'+t+'.csv',  index=None, header=False)
                
data = [picking_lines, measurements, configurations, search_logic_name, order1, order2, timing]
#print(data)

#print to csv
file = open('20190328_greedy_stop_span_assignment_all_files.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(data)
                    
print('\007')
print('Metta!')