# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 15:53:48 2018

@author: 20304269
"""

#20180919_variable_neighbourhood_parameter_calibration.py

import pandas as pd
import numpy as np
import random
import copy
import time
from datetime import timedelta

#INPUT: cost matrix (for batches of 2)
file_names = [ '20130227_1065_LOLS_2', '20130424_3142_LOLS_8', \
             '20130327_1966_LOMS_2', '20130312_1569_LOMS_3', \
             '20130228_1079_LOSS_8', '20130326_1942_LOSS_5', \
             '20130216_701_MONS_5', '20130220_790_MONS_5', '20130225_910_MONS_2', \
             '20130416_2710_SOLS_6', '20130208_517_SOMS_6', '20130223_867_SOSS_5']

for f in file_names: 
    
    extension_names = ['_stops', '_nonidentical_stops' , '_orders_serviced_bays', '_min_span', '_nonidentical_span', '_orders_serviced_spans', '_span_ratio']
    for g in extension_names:
             
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
        
        ##Variable neighbourhood search
        ##TODO: either random or greedy
        ###random solution
        o = df.columns.tolist() 
        random_solution = random.sample(o, len(o))
        initial_solution = random_solution + [random_solution[0]]
        ##greedy
        # o = 
        # intial_solution = o + [o[0]]       
             
        #0: Cost function
        def cost(solu):
            order_1 = solu[:int(len(solu)/2)]
        #    print('order_1', order_1)
            order_2 = solu[int(len(solu)/2):] #last element is excluded
        #    print('order_2', order_2)
            comp = []
          
            for i,j in zip(order_1, order_2):
                comp.append(df.loc[i][j])
               
            cost = sum(comp)
            return cost
        
        #0: Cost component function
        def cost_component(solu):   
        
            order_1 = solu[:int(len(solu)/2)]
            order_2 = solu[int(len(solu)/2):] #last element is excluded
            comp = []
            component = []
          
            for i,j in zip(order_1, order_2):
                comp.append(df.loc[i][j])
                component.append([i,j])
        #        print(i)
        #        print(j)
        #        print('comp', comp[-1])        
        
            s = pd.Series(comp).sort_values(ascending=False) #https://stackoverflow.com/questions/48738249/how-to-get-the-original-indexes-after-sorting-a-list-in-python (pandas)
            sorted_index = s.index.tolist()
            
            return component, sorted_index
            
        def neighbourhood_swap_first(solu): #neighbourhood that swaps the highest cost with the lowest (even)
            
            cost_comp = cost_component(solu)
            
            component = cost_comp[0]
            sorted_index = cost_comp[1]
            
            #index of pair
            i1 = sorted_index[0]
            i2 = sorted_index[-1]
        #    print(component[i1][1])
        #    print(component[i2][1])
            
            a, b = solu.index(component[i1][1]), solu.index(component[i2][1])
            solu[a], solu[b] = solu[b], solu[a]
            
        #    print('solu', solu)
            return solu           
        
        def neighbourhood_swap_second(solu): #neighbourhood that swaps the second highest cost with the lowest (even)
            
            cost_comp = cost_component(solu)
            
            component = cost_comp[0]
            sorted_index = cost_comp[1]
            
            #index of pair
            i1 = sorted_index[1]
            i2 = sorted_index[-2]
        #    print(component[i1][1])
        #    print(component[i2][1])
            
            a, b = solu.index(component[i1][1]), solu.index(component[i2][1])
            solu[a], solu[b] = solu[b], solu[a]
            
        #    print('solu', solu)
            return solu
        
        def neighbourhood_swap_third(solu): #neighbourhood that swaps the third highest cost with the lowest (even)
            
            cost_comp = cost_component(solu)
            
            component = cost_comp[0]
            sorted_index = cost_comp[1]
            
            #index of pair
            i1 = sorted_index[2]
            i2 = sorted_index[-3]
        #    print(component[i1][1])
        #    print(component[i2][1])
            
            a, b = solu.index(component[i1][1]), solu.index(component[i2][1])
            solu[a], solu[b] = solu[b], solu[a]
            
        #    print('solu', solu)
            return solu            
        
        #0: Generate random initial solution
        def generate_random_solution(o):
            random_solution = random.sample(o, len(o)) 
            initial_solution = random_solution + [random_solution[0]] #include start and end of solution
        #    print('initial solution', initial_solution)
            return initial_solution
        
        initial_solution = generate_random_solution(o)
#        print('initial_solution', initial_solution)   
          
        #Local search with two_opt (alternatives: insertion neighborhood)
            #Requirements: start and end element are similar    
        def local_search(solu): #http://pedrohfsd.com/2017/08/09/2opt-part1.html
            best = solu
        #    print('best_start', best)
            improved = True #first termination criterion  
            
            while improved:
                improved = False          
                               
                for i in range(1, iterations-2):
        #            print(range(1, 4-2))
        #            print('i', i)
                    for j in range(i+1, iterations):               
        #                print(range(i+1, iterations))
        #                print('j', j)
                        if j-i == 1: #changes nothing, skip
                            continue
                        
                        new_solution = solu[:]
                        new_solution[i:j] = solu[j-1:i-1:-1] #two-opt swap
        #                print('cost(new_solution)', cost(new_solution))
        #                print('cost(best)', cost(best))
                        if cost(new_solution) < cost(best):
        #                    print('yes')
                            best = copy.deepcopy(new_solution)
                            improved = True
        #                    print('better than previous solution', improved)              
                                   
        #        print('improved range', improved) #stops when improved is false!
                solu = best
        #        print('best', best)    
            return best
        
        #parameter calibrations
        con = list(range(1,7))
        config = [str(l) for l in con]
        
        count_max = [5,10,15]
        
        iterations = [5,10]
        
        for l, m, n in zip(config, count_max, iterations):
            
            config = l
            count_max = m
            iterations = n 
            
            #timing
            start_time = time.time()
        
            #number of neighbourhood structures
            k_max = 3 
            
            def variable_neighbourhood_search(initial_solution, count_max, k_max):           
                
                best_solution = initial_solution
#                print('initial_algorithm', best_solution)
                         
                count = 0
            #    not_accepted = 0 
                
                while not count == count_max:    
                                              
#                    print('count_initial', count)
#                    print('count max', count_max)          
                    k = 0
                    
                    while k < k_max:
                        
#                        print('count', count)
                        
                        #Step1: Shaking (select a random solution s' in the neighbourghood of s)
                        initial_random = generate_random_solution(o)
#                        print('initial_random', initial_random)
#                        print('k', k)
                        
                        if k == 0:
                            random_solution = neighbourhood_swap_first(initial_random)
                            
                        elif k == 1:
                            random_solution = neighbourhood_swap_second(initial_random)                
                        elif k == 2:
                            random_solution = neighbourhood_swap_third(initial_random)          
#                        print('random_solution', random_solution)
                        test_solution = local_search(random_solution)
#                        print('test_solution', test_solution)
                        
                        if cost(test_solution) < cost(best_solution):
#                            print('yes')
                            best_solution = test_solution
#                            print('new_best', best_solution)
                            k = 0
                        
                        else:
#                            print('next_neighbourhood')
                            k +=1
                        
                    count +=1
                
                return best_solution  
            
            ##timing   
            elapsed_time_secs = time.time() - start_time
            msg = "Execution took: %s secs (Wall clock time)" % timedelta(seconds=round(elapsed_time_secs))
            #print(msg)
            
            #OUTPUT:                                                  
            solution_orders = variable_neighbourhood_search(initial_solution, count_max, k_max)
            orders = solution_orders[:-1]     
            
            order_1 = orders[:int(len(orders)/2)]
            order_2 = orders[int(len(orders)/2):] 
#            print(order_1)  
#            print(order_2) 
            
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
            
            #OUTPUT order batching
            file = open(f+'_time'+g+'_VNS_config_'+l+'.txt', 'w')
            file.writelines('Picking line: ')
            file.writelines(f)
            file.writelines('\n')
            file.writelines(msg)
            file.close() 
                
            batched.to_csv(f+'_batched_2'+g+'_VNS_config_'+l+'.csv',  index=None, header=False)

print('\007')        
print('Jup!!!!!!!')
            
