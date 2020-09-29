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

#INPUT: cost matrix (for batches of 2)
filenames = ['20130425_3150_LOLS_2' , '20130227_1065_LOLS_2', '20130422_2964_LOLS_5', '20130403_2137_LOLS_2', '20130403_2147_LOLS_2', \
             '20130422_2968_LOLS_5', '20130320_1756_LOLS_2', '20130209_521_LOLS_2', '20130222_852_LOLS_2', '20130223_1814_LOLS_5', \
             '20130424_3142_LOLS_8', '20130406_2459_LOLS_2', '20130322_1810_LOLS_2', '20130326_1865_LOLS_2', '20130410_2913_LOMS_2', \
             '20130322_1813_LOMS_8', '20130327_1966_LOMS_2', '20130307_1503_LOMS_5', '20130207_505_LOMS_2', '20130326_1868_LOMS_5', \
             '20130225_926_LOMS_2', '20130409_2503_LOMS_2', '20130411_2569_LOMS_2', '20130312_1569_LOMS_3', '20130418_2817_LOMS_8', \
             '20130307_1460_LOMS_8', '20130219_779_LOSS_2', '20130307_1501_LOSS_2', '20130228_1079_LOSS_8', '20130228_1080_LOSS_5', \
             '20130409_2502_LOSS_5', '20130410_2550_LOSS_8', '20130415_2640_LOSS_5', '20130403_2037_LOSS_3', '20130313_1605_LOSS_2', \
             '20130318_1715_LOSS_5', '20130216_704_LOSS_2', '20130326_1942_LOSS_5', '20130402_2022_MONS_2', '20130216_701_MONS_5', \
             '20130212_591_MONS_2', '20130220_790_MONS_5', '20130212_592_MONS_2', '20130225_910_MONS_2', '20130416_2710_SOLS_6', \
             '20130214_669_SOMS_7', '20130208_517_SOMS_6', '20130223_867_SOSS_5', '20130208_515_SOSS_5', '20130425_3160_SOLS_6']

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
#        print('New df with dummy 100:')  
#        print(df.head(3))
        
        #INPUT for batching
        df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
        branch_list = df2['BRANCH_NO'].tolist()   
  
        ###great deluge algorithm
        #Step 1: Choose initial configuration (random or greedy) 
        o = df.columns.tolist()
        sol = random.sample(o, len(o)) #random
    #    print('sol', sol)
        
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
    #                print('old_cost', cost(solution))            
                    new_solution = neighbour(solution) #choose new config
                    new_cost = cost(new_solution) #E or new config
    #                print('new_cost', new_cost)
                    if old_cost <= new_cost:
    #                    print('old_cost_in_if', old_cost)
    #                    print('new_cost_in_if', new_cost)
                        count += 1 #TODO: Why constant count??
    #                    print('count', count)
                    if new_cost < initial_water_level:
                        count = 0
                        solution = copy.deepcopy(new_solution) #accepting new_solution
                        initial_water_level = initial_water_level - (initial_water_level - new_cost)/par
                        if cost(solution) < cost(best_solution):
                            best_solution = copy.deepcopy(solution)        
                    i += 1
    #                print('i', i)
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
print('Last one down')


    

