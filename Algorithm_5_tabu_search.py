# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 13:56:06 2018

@author: 20304269
"""

#source: 20181129_Tabu_search_rand_greedy_all_files_config_5_updated.py
##changed time measurement

#INSPIRATION: https://www.techconductor.com/algorithms/python/Search/Tabu_Search.php

import pandas as pd
import numpy as np
import copy
import time
import json
import csv

#Lists for output
picking_lines = []
measurements = []
configurations = []
initial_solutions = []
timing = []

#INPUT: file names of cost matrix (for batches of 2)
filenames = ['20130425_3150_LOLS_2']

for f in filenames:
    
    extension_names = ['_stops', '_nonidentical_stops' , '_orders_serviced_bays', '_min_span', '_nonidentical_span', '_orders_serviced_spans', '_span_ratio']
    for g in extension_names:
        
        con = list(range(1,6))
        config = [str(l) for l in con]
            
        for s in config:
             
            #INPUT for algorithm
            df = pd.read_csv(f+g+'.csv', index_col=0, header=0)
            df.columns = df.columns.astype(int)
            if not int(len(df.columns)) % 2 == 0:
                    df[11111] = int(100)
                    df.loc[11111] = int(100)
            for col in df:
                df.loc[col, col] = np.nan 
            
            #Change matrix to dictonary: Generating a dictonary of neighbours and the cost with each neighbour
            d = df.to_dict('dict')
            dict_of_neighbours = {k1:{k:v for k,v in v1.items() if not pd.isnull(v)} for k1, v1 in d.items()} #https://stackoverflow.com/questions/44569021/removing-nan-from-dictionary-inside-dictionary-dict-changes-size-during-runtime
            
            #INPUT for batching
            df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
            branch_list = df2['BRANCH_NO'].tolist()
            
            ##Tabu search algorithm
            #Step1: Generate a greedy solution                        
            ##INPUT greedy
            data2 = pd.read_csv('rtb_assignment_random_all_files_input.csv', names = ['PICKING_LINES', 'MEASUREMENTS', 'SEARCH_LOGIC_NAME', 'CONFIGURATIONS', 'ORDER1', 'ORDER2', 'TIMING'], dtype={'CONFIGURATIONS': str})
            
            order1_initial = data2.loc[(data2['PICKING_LINES'] == f) & (data2['MEASUREMENTS'] == g) & (data2['CONFIGURATIONS'] == s), 'ORDER1'].iloc[0]
            order2_initial = data2.loc[(data2['PICKING_LINES'] == f) & (data2['MEASUREMENTS'] == g) & (data2['CONFIGURATIONS'] == s), 'ORDER2'].iloc[0]
            order1_ = json.loads(order1_initial)
            order2_ = json.loads(order2_initial)   
            
            initial_order_ = result = [None]*(len(order1_)+len(order2_)) #https://stackoverflow.com/questions/3678869/pythonic-way-to-combine-two-lists-in-an-alternating-fashion
            initial_order_[::2] = order1_
            initial_order_[1::2] = order2_      
    #        print(initial_order_)
            initial_order_first = initial_order_[0]
            initial_order__ = initial_order_ + [initial_order_first]
            initial_solution = [int(j) for j in initial_order__]
    #        print('sol_greedy', sol_greedy)        
                
            ##Initialise
            #first solution - Requirement: same start and end node
            first_solution = initial_solution

            #distance of first solution
            first_solution_ = []
            for d in first_solution:
                first_solution_.extend([d,d])
            first_solution__ = first_solution_[1:-1]
            order_1 = first_solution__[::2]
            order_2 = first_solution__[1::2]
            components = []  
            for i,j in zip(order_1, order_2):
                components.append(df.loc[i][j])
            distance_of_first_solution = sum(components)
            #print('distance_of_first_solution', distance_of_first_solution)       
                       
            #Generate a neighbourhood: sorted by total distance from highest to lowest; 
            #1-1 exchange method (exchange each node in a solution with each other node and generating a number of solution named neighbourhood)       
            def find_neighbour(solution, index_of_best_solution, dict_of_neighbours):

                neighbourhood_of_solution = []
            
                n = solution[1:-1][index_of_best_solution]
            
                idx1 = solution.index(n)
                for kn in solution[1:-1]:
                    
                    if not neighbourhood_of_solution == []:
                        break        
            
                    idx2 = solution.index(kn)
                    if n == kn: # if n and kn are the same, continue until you find different ones
                        continue
            
                    _tmp = copy.deepcopy(solution) #copy of current solution, switch index one and two in copied solution
                    _tmp[idx1] = kn 
                    _tmp[idx2] = n 
            #        print('copied solution index one and two switched', _tmp)
            
                    distance = 0
            
                    for k in _tmp[:-1]:
            #            print('copied solution without end_node', _tmp[:-1])
                        next_node = _tmp[_tmp.index(k) + 1]
                        for j in dict_of_neighbours[k]:
                            i = [j, dict_of_neighbours[k][j]]
                            if i[0] == next_node:
                                distance = distance + float(i[1])
                    _tmp.append(distance)

                    if _tmp not in neighbourhood_of_solution:
                        neighbourhood_of_solution.extend(_tmp)
               
                return neighbourhood_of_solution            
            
            #Parameter from calibration                       
            iters = 5
            per = 0.1            
            unchanged_max = 3           
                                                                         
            ##Length of tabu list (can never be larger than list of orders!)
            length = int(len(df.columns) * per)
            
            def tabu_search(first_solution, distance_of_first_solution, dict_of_neighbours, iters, length, unchanged_max):
                
                count = 1
                solution = first_solution
                tabu_list = list()
                best_cost = distance_of_first_solution
                best_solution_ever = solution
                unchanged = 0
                
                ##timing
                start_time = time.time()
            
                while count <= iters:
                    index_of_best_solution = 0
                    best_solution = find_neighbour(solution, index_of_best_solution, dict_of_neighbours)
                    
                    if unchanged == unchanged_max:
                        break                    
            
                    found = False
                    while found is False:
                        i = 0
                        while i < len(best_solution): #to find a new node to exchange
            #                print('i', i)
                            if best_solution[i] != solution[i]: #if it is different nodes break
                                first_exchange_node = best_solution[i]
                                second_exchange_node = solution[i]
                                break
                            i = i + 1 #if it is the same node, loop again (usually first one is the same due to same start in neighbourhood)
            #            print('found', found) #found is FALSE since no new best_solution has been found
            #            print('tabu_list', tabu_list) #tabu_list includes exchange nodes now
                        if [first_exchange_node, second_exchange_node] not in tabu_list and [second_exchange_node, first_exchange_node] not in tabu_list: #check if exchange nodes are not in the tabu_list                                                   
                            tabu_list.append([first_exchange_node, second_exchange_node])
            #                print('tabu_list', tabu_list) #tabu_list includes exchange nodes now
                            found = True
                            solution = best_solution[:-1] 
                            cost = best_solution[-1]               
                            unchanged += 1
                            if cost < best_cost: #only change if cost of new solution is better than cost of best_solution_ever (save best_solution_ever)
                                best_cost = cost
                                best_solution_ever = solution
                                unchanged = 0
                        else:
                            index_of_best_solution = index_of_best_solution + 1
            #                print('index_of_best_solution', index_of_best_solution) #TO DO: include further search if necessary
                            best_solution = find_neighbour(solution, index_of_best_solution, dict_of_neighbours)
            
                    if len(tabu_list) >= length:
            #            print('tabu_list_shortened', tabu_list)
                        tabu_list.pop(0)
            
                    count = count + 1
                    
                ##timing   
                elapsed_time_secs = time.time() - start_time
            
                return best_solution_ever, best_cost, elapsed_time_secs
            
            #OUTPUT
            tabu_search = tabu_search(first_solution, distance_of_first_solution, dict_of_neighbours, iters, length, unchanged_max)
#            print('tabuuu', tabu_search)
            sol = tabu_search[0]
            orders = sol[:-1] #https://stackoverflow.com/questions/23130300/split-list-into-2-lists-corresponding-to-every-other-element
            #print(orders)
            times = tabu_search[-1]
#            print(times)
                                  
            order_1 = orders[::2] 
            order_2 = orders[1::2]
#           print('order_1', order_1)
#           print('order_2', order_2)            

            #--------------------
            picking_lines.append(f)
            measurements.append(g)
            configurations.append(s)
            initial_solutions.append('_greedy')
            timing.append(times)
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
            
                
            batched.to_csv(f+'_batched_2'+g+'_tabu_config_'+s+'.csv',  index=None, header=False)
            
results = [picking_lines, measurements, configurations, initial_solutions, timing]

#print to csv
file = open('Tabu_all_files_output_config_5.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(results)

print('\007')        
print('Metta!')

        
        
        
