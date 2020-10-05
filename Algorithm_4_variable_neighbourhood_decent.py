# -*- coding: utf-8 -*-
"""
@author: 20304269
Created on Fri Sep 14 15:53:48 2018

"""

#source: 20181129_variable_neighbourhood_decent_rand_greedy_all_files_config_5_updated.py

import pandas as pd
import numpy as np
import copy
import timeit
import json
import csv

#Lists for output
picking_lines = []
measurements = []
configurations = []
initial_solutions = []
timing = []

#Wrapper function for timining
def wrapper(func, *args, **kwargs):
    def wrapped():
        return func(*args, **kwargs)
    return wrapped

#INPUT: file names of cost matrix (for batches of 2)
filenames = ['20130425_3150_LOLS_2']

for f in filenames:
    
    extension_names = ['_stops', '_nonidentical_stops' , '_orders_serviced_bays', '_min_span', '_nonidentical_span', '_orders_serviced_spans', '_span_ratio']
    for g in extension_names:
        
        con = list(range(1,6))
        config = [str(l) for l in con]
            
        for s in config:
             
            #INPUT for algorithm
    #        print('name of picking_line:', f+g)
            df = pd.read_csv(f+g+'.csv', index_col=0, header=0)
            df.columns = df.columns.astype(int)
            if not int(len(df.columns)) % 2 == 0:
                    df[11111] = int(100)
                    df.loc[11111] = int(100)
            for col in df:
                df.loc[col, col] = np.nan 
            #print(df)
            
            #INPUT for batching
            df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
            branch_list = df2['BRANCH_NO'].tolist()
            
            ##Variable neighbourhood descent
            #parameters
            count_max = 50 #termination criterion
            k_max = 3 #neighbourhood size            
            
            #Step: Generate a random solution (random or greedy)
                        
            ##INPUT greedy
            data2 = pd.read_csv('rtb_assignment_random_all_files_input.csv', names = ['PICKING_LINES', 'MEASUREMENTS', 'SEARCH_LOGIC_NAME', 'CONFIGURATIONS', 'ORDER1', 'ORDER2', 'TIMING'], dtype={'CONFIGURATIONS': str})
            
            order1_initial = data2.loc[(data2['PICKING_LINES'] == f) & (data2['MEASUREMENTS'] == g) & (data2['CONFIGURATIONS'] == s), 'ORDER1'].iloc[0]
            order2_initial = data2.loc[(data2['PICKING_LINES'] == f) & (data2['MEASUREMENTS'] == g) & (data2['CONFIGURATIONS'] == s), 'ORDER2'].iloc[0]
            order1_ = json.loads(order1_initial)
            order2_ = json.loads(order2_initial)
            
            initial_order_ = order1_ + order2_ 
            initial_solution = [int(j) for j in initial_order_]
    #        print('sol_greedy', sol_greedy)          
                    
            #0a: Cost function
            def cost(solu):
                order_1 = solu[:int(len(solu)/2)]
                order_2 = solu[int(len(solu)/2):] 
                comp = []
              
                for i,j in zip(order_1, order_2):
                    comp.append(df.loc[i][j])       
                cost = sum(comp)
                return cost            
            
            #0b: Cost component function
            def cost_component(solu): 
                order_1 = solu[:int(len(solu)/2)]
                order_2 = solu[int(len(solu)/2):]
                comp = []
                component = []
              
                for i,j in zip(order_1, order_2):
                    comp.append(df.loc[i][j])
                    component.append([i,j]) 
                s = pd.Series(comp).sort_values(ascending=False) #https://stackoverflow.com/questions/48738249/how-to-get-the-original-indexes-after-sorting-a-list-in-python (pandas)
                sorted_index = s.index.tolist()
                
                return component, sorted_index
            
            #0c: Defining the neighbourhood structure                
            def neighbourhood_swap_first(solu): #neighbourhood that swaps the highest cost with the lowest (even)
                
                cost_comp = cost_component(solu)    
                component = cost_comp[0]
                sorted_index = cost_comp[1]
                
                #index of pair
                i1 = sorted_index[0]
                i2 = sorted_index[-1]
                a, b = solu.index(component[i1][1]), solu.index(component[i2][1])
                solu[a], solu[b] = solu[b], solu[a]    
            
                return solu
            
            def neighbourhood_swap_second(solu): #neighbourhood that swaps the second highest cost with the lowest (even)
                
                cost_comp = cost_component(solu)    
                component = cost_comp[0]
                sorted_index = cost_comp[1]
                
                #index of pair
                i1 = sorted_index[1]
                i2 = sorted_index[-2]   
                a, b = solu.index(component[i1][1]), solu.index(component[i2][1])
                solu[a], solu[b] = solu[b], solu[a]
                
                return solu
            
            def neighbourhood_swap_third(solu): #neighbourhood that swaps the third highest cost with the lowest (even)
                
                cost_comp = cost_component(solu)    
                component = cost_comp[0]
                sorted_index = cost_comp[1]
                
                #index of pair
                i1 = sorted_index[2]
                i2 = sorted_index[-3]
                
                a, b = solu.index(component[i1][1]), solu.index(component[i2][1])
                solu[a], solu[b] = solu[b], solu[a]
                
                return solu
             
            #Local search with two_opt swap (alternatives: insertion neighborhood) [component 2]
                #INSPIRATION - P. Diniz: http://pedrohfsd.com/2017/08/09/2opt-part1.html
                #!REQUIREMENTS! - same start and end element (TSP)
            def local_search(solu): 
                solu = solu + [solu[0]]
                best = solu
                improved = True #first termination criterion
                iterations = 5  #second termination criterion to reduce runtime
                
                while improved:
                    improved = False          
                                   
                    for i in range(1, iterations-2):
                        for j in range(i+1, iterations):               
                            if j-i == 1: #no changes, continue
                                continue                
                            new_solution = solu[:]
                            new_solution[i:j] = solu[j-1:i-1:-1] #two-opt swap
                            if cost(new_solution) < cost(best):
                                best = copy.deepcopy(new_solution)
                                improved = True
                    solu = best
                    
                best = best[:-1]
                
                return best
            
            #VND algorithm
            def variable_neighbourhood_descent(initial_solution, count_max, k_max):          
                
                count = 0       
                k = 0
                best_solution = initial_solution
                        
                while k < k_max:
                    
                    if count >= count_max:
                        print('break')
                        break
                    
                    #Step1: Select a best solution s' in the neighbourghood of s
                    sol = copy.deepcopy(best_solution)        
                    if k == 0:
                        curr_solution = neighbourhood_swap_first(sol)            
                    elif k == 1:
                        curr_solution = neighbourhood_swap_second(sol)                
                    elif k == 2:
                        curr_solution = neighbourhood_swap_third(sol)            
                        
                    test_solution = local_search(curr_solution)
                    
                    #Step2: Is the solution s' in this neighbourhood better than the current best solution?        
                    if cost(test_solution) < cost(best_solution): #yes, new best solution
                        best_solution = copy.deepcopy(test_solution)
                        k = 0               
                                    
                    else: #no, next neighbourhood
                        k +=1
                        
                    count +=1 #termination counter  
                           
                return best_solution
                      
            #OUTPUT                          
            orders = variable_neighbourhood_descent(initial_solution, count_max, k_max)
            
            order_1 = orders[:int(len(orders)/2)]
            order_2 = orders[int(len(orders)/2):] 
    #            print(order_1)  
    #            print(order_2)
    
    #       #timinig
            wrapped = wrapper(variable_neighbourhood_descent, initial_solution, count_max, k_max)
            time = timeit.timeit(wrapped, number = 1)

            #--------------------
            picking_lines.append(f)
            measurements.append(g)
            configurations.append(s)
            initial_solutions.append('_greedy')
            timing.append(time)
            #--------------------
            
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
    #            print('length of batch_list', len(batch_list))
              
            #Group by batch and sum up entries
            batched = df2.groupby(['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO','BATCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE'], as_index=False).agg \
             ({'BRANCH_NO' : 'count', \
              'NUM_UNITS_y' : 'sum', \
              'NUM_BRANCHES' : 'sum', \
              'NUM_UNITS_x' : 'sum', \
              'VOLUME_PER_UNIT' : 'sum',\
              'WEIGHT_PER_UNIT_KG' : 'sum'})
                
            batched.to_csv(f+'_batched_2'+g+'_VND_config_'+s+'.csv',  index=None, header=False)
            
results = [picking_lines, measurements, configurations, initial_solutions, timing]

#print to csv
file = open('VND_all_files_output_config_5_updated.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(results)
    
print('\007')        
print('Metta')
            
