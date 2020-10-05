# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 13:24:32 2018

@author: 20304269
"""

#20180912_Simulated_perturbations_parameter_all_files.py

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
    
    extension_names = ['_stops', '_nonidentical_stops' , '_orders_serviced_bays', '_min_span', '_nonidentical_span', '_orders_serviced_spans', '_span_ratio']
    for g in extension_names:
             
        #INPUT for algorithm
        print('name of picking_line:', f+g)
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
        batch_list = []
        
        ###simulated annealing algorithm:
        #INSPIRATION: http://katrinaeg.com/simulated-annealing.html
        
        #Step1: Generate a random solution (random or greedy)
        o = df.columns.tolist()
        sol = random.sample(o, len(o)) #random
        
        #Step2: Calculate cost 
        #@profile
        def cost(sol):
            order_1 = sol[:int(len(sol)/2)]
            order_2 = sol[int(len(sol)/2):]
            comp = []
          
            for i,j in zip(order_1, order_2): #http://treyhunner.com/2016/04/how-to-loop-with-indexes-in-python/
                comp.append(df.loc[i][j])
            cost = sum(comp)
            return cost
        
        #Step3: Generate a random neighboring solution 
        #@profile
        def neighbour(sol):
            idx = range(len(sol))
            i1, i2 = random.sample(idx, 2)
            sol[i1], sol[i2] = sol[i2], sol[i1]
            return sol
    
        #Acceptance probability
        #@profile
        def acceptance_probability(old_cost, new_cost, T):
            if new_cost < old_cost:
                return 1.0
            else:
                return np.exp((old_cost - new_cost) / T)
        
        #timing
        start_time = time.time()
        
        #Parameter from calibration
        T_initial = 56.0
        c_max = 5
        i_max = 1000
        alpha = 0.9
        accepted_max = 12
        
        #@profile 
        def anneal(sol):
            solution = copy.deepcopy(sol)
            best_solution = copy.deepcopy(solution)
            old_cost = cost(solution)
            T = T_initial #Initial temperature
            T_min = 0.0001 #Temperature doesn't go below 0
            
            count = 0 #goes up if no further improvement is made (no new solution is accepted)
            accepted = 0 #decreases temperature
            i = 1 #iterations up to 100
            while i <= i_max:
#                    print('count beginning', count)
                if T == T_min:
#                        print('Temperature termination')
                    break
                
#                    print('Old_cost:', old_cost)
                new_solution = neighbour(solution)
                new_cost = cost(new_solution)
#                    print('New_cost', new_cost)
                ap = acceptance_probability(old_cost, new_cost, T)
                if ap > random.random():
                    count = 0
                    accepted +=1
#                        print('accepted', accepted)
                    solution = copy.deepcopy(new_solution)
                    old_cost = copy.deepcopy(new_cost)            
#                        print('New old_cost:', old_cost)
                    if cost(solution) < cost(best_solution):
                        best_solution = copy.deepcopy(solution)
                else: 
                    accepted = 0
                    count += 1
#                        print('not accepted', count)
                
                if count == c_max: #terminate after 3 successive temperature stages without any acceptance
#                        print('terminate')
                    break
        
                i += 1
#                    print('i', i)
                if accepted == accepted_max:
                    accepted = 0
                    T = T*alpha
#                        print('Current temperature', T)
            return best_solution
    
        ##timing   
        elapsed_time_secs = time.time() - start_time
        msg = "Execution took: %s secs (Wall clock time)" % timedelta(seconds=round(elapsed_time_secs))
        #print(msg)
        
        #OUTPUT   simulated annealing  
        best_solution = anneal(sol)
        
        order_1 = best_solution[:int(len(sol)/2)]
        order_2 = best_solution[int(len(sol)/2):]
#            print('order_1', order_1)
#            print('order_2', order_2)
                      
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
          
        #Group by batch and sum up entries
        batched = df2.groupby(['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO','BATCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE'], as_index=False).agg \
         ({'BRANCH_NO' : 'count', \
          'NUM_UNITS_y' : 'sum', \
          'NUM_BRANCHES' : 'sum', \
          'NUM_UNITS_x' : 'sum', \
          'VOLUME_PER_UNIT' : 'sum',\
          'WEIGHT_PER_UNIT_KG' : 'sum'})
        
        #OUTPUT order batching
        file = open(f+'_time'+g+'_sa_perturbations.txt', 'w')
        file.writelines('Picking line: ')
        file.writelines(f)
        file.writelines('\n')
        file.writelines(msg)
        file.close() 
            
        batched.to_csv(f+'_batched_2'+g+'_sa_perturbations.csv',  index=None, header=False) 

print('\007')        
print('Metta')       
            

        