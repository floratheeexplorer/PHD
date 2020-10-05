# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 09:28:40 2018

@author: 20304269
"""

#20180912_Great_deluge_all_files.py

import pandas as pd
import numpy as np
import random
#import line_profiler
import copy
import time
from datetime import timedelta

#INPUT: file names of cost matrix (for batches of 2)
filenames = ['20130425_3150_LOLS_2']

for f in filenames:
    
    extension_names = ['_stops', '_nonidentical_stops', '_orders_serviced_bays', '_min_span', '_nonidentical_span', '_orders_serviced_spans', '_span_ratio']
    for g in extension_names:
             
        #INPUT for algorithm
#        print('name of picking_line:', f+g)
        df = pd.read_csv(f+g+'.csv', index_col=0, header=0)
        df.columns = df.columns.astype(int)
        if not int(len(df.columns)) % 2 == 0:
                df[11111] = int(100)
                df.loc[11111] = int(100)
        for col in df:
            df.loc[col, col] = 100
        
        #INPUT for batching
        df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
        branch_list = df2['BRANCH_NO'].tolist()   
  
        ###great deluge algorithm
        #Step 1: Choose initial configuration (random or greedy) 
        o = df.columns.tolist()
        sol = random.sample(o, len(o)) #random
        
        #Step 0a: Calculate water-level aka cost
        def cost(sol):
            order_1 = sol[:int(len(sol)/2)]
            order_2 = sol[int(len(sol)/2):]
            comp = []  
            for i,j in zip(order_1, order_2):
                comp.append(df.loc[i][j])       
            cost = sum(comp)
            return cost
        
        #Step 0b: Generate neighbouring solution: Swap two orders around
        def neighbour(sol):
            idx = range(len(sol))
            i1, i2 = random.sample(idx, 2)
            sol[i1], sol[i2] = sol[i2], sol[i1]
            return sol
        
        #Parameter from calibration
        par = 0.5
        c_max = 5
        i_max = 100             
            
        #timing
        start_time = time.time()
        
        #@profile
        def greatdeluge(sol):
            solution = copy.deepcopy(sol)
            best_solution = copy.deepcopy(solution)
            initial_water_level = cost(solution)
            
            #Termination of algorithm:    
            i = 1 #count for too many iterations - after 100 
            count = 0 #count for no increase in quality - after 5     
            while i < i_max: 
    #            print('begin counting', count)                      
                if count == c_max: 
                    break
                else:            
    #                print('Current water level:', initial_water_level)
                    old_cost = copy.deepcopy(cost(solution)) #included old_cost to compare in if, otherwise constant count         
                    new_solution = neighbour(solution) #choose new config
                    new_cost = cost(new_solution) #E or new config  
                    if old_cost <= new_cost:
                        count += 1 
                    if new_cost < initial_water_level:
                        count = 0
                        solution = copy.deepcopy(new_solution) #accepting new_solution
                        initial_water_level = initial_water_level - (initial_water_level - new_cost)/par
                        if cost(solution) < cost(best_solution):
                            best_solution = copy.deepcopy(solution)        
                    i += 1
            return best_solution
        
        ##timing   
        elapsed_time_secs = time.time() - start_time
        msg = "Execution took: %s secs (Wall clock time)" % timedelta(seconds=round(elapsed_time_secs))
        #print(msg)
    
        #OUTPUT great deluge
        best_solution = greatdeluge(sol)
        
        order_1 = best_solution[:int(len(sol)/2)]
        order_2 = best_solution[int(len(sol)/2):]
        #print('order_1', order_1)
        #print('order_2', order_2)       
     
        ###order batching (size of 2)
        batch_list=[]
        for y in range(len(branch_list)):
            for x in range(len(order_1)):
                if branch_list[y] == order_1[x]:
                    batch_list.append('Batch {}'.format(x+1))
                elif branch_list[y] == order_2[x]:
                    batch_list.append('Batch {}'.format(x+1))
    
        ##Third column with 'BATCH_NO'
        df2['BATCH_NO'] = batch_list  
          
        #Group by batch and sum up entries
        batched = df2.groupby(['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO','BATCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE'], as_index=False).agg \
         ({'BRANCH_NO' : 'count', \
          'NUM_UNITS_y' : 'sum', \
          'NUM_BRANCHES' : 'sum', \
          'NUM_UNITS_x' : 'sum', \
          'VOLUME_PER_UNIT' : 'sum',\
          'WEIGHT_PER_UNIT_KG' : 'sum'})
        
        #OUTPUT order batching
        file = open(f+'_time'+g+'_gd.txt', 'w')
        file.writelines('Picking line: ')
        file.writelines(f)
        file.writelines('\n')
        file.writelines(msg)
        file.close() 
        
        batched.to_csv(f+'_batched_2'+g+'_gd.csv',  index=None, header=False) 
    
print('\007')
print('Metta!')


    

