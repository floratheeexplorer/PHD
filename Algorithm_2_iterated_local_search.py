# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 15:53:48 2018

@author: 20304269
"""

#20181128_Iterated_local_search_rand_greedy_all_files_config_5_updated.py

import pandas as pd
import numpy as np
import random
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
    
    extension_names = ['_stops' , '_nonidentical_stops' , '_orders_serviced_bays', '_min_span', '_nonidentical_span', '_orders_serviced_spans', '_span_ratio']
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
            o = df.columns.tolist() #to generate restart solution
            
            #INPUT for batching
            df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
            branch_list = df2['BRANCH_NO'].tolist()
            
            ##Iterated local search
            #Step1: Generate a greedy solution (random or greedy)                        
            ##INPUT greedy
            data2 = pd.read_csv('rtb_assignment_random_all_files_input.csv', names = ['PICKING_LINES', 'MEASUREMENTS', 'SEARCH_LOGIC_NAME', 'CONFIGURATIONS', 'ORDER1', 'ORDER2', 'TIMING'], dtype={'CONFIGURATIONS': str})
            
            order1_initial = data2.loc[(data2['PICKING_LINES'] == f) & (data2['MEASUREMENTS'] == g) & (data2['CONFIGURATIONS'] == s), 'ORDER1'].iloc[0]
            order2_initial = data2.loc[(data2['PICKING_LINES'] == f) & (data2['MEASUREMENTS'] == g) & (data2['CONFIGURATIONS'] == s), 'ORDER2'].iloc[0]
            order1_ = json.loads(order1_initial)
            order2_ = json.loads(order2_initial)
            
            initial_order_ = order1_ + order2_
            initial_solution = [int(j) for j in initial_order_]
    #        print('sol_greedy', sol_greedy)  

            #Parameters for iterated local search
            count_max = 5 #termination criterion
            not_accepted_max = 3 #history criterion                                          
           
            #Cost function
            def cost(solu):
                order_1 = solu[:int(len(solu)/2)]
                order_2 = solu[int(len(solu)/2):]
                comp = []
              
                for i,j in zip(order_1, order_2):
                    comp.append(df.loc[i][j])
                cost = sum(comp)
                return cost
            
            #Local search with two_opt swap (alternatives: insertion neighborhood)
                #INSPIRATION - P. Diniz: http://pedrohfsd.com/2017/08/09/2opt-part1.html
                ##REQUIREMENTS - same start and end element (TSP)
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
            
            #Perturbation with double bridge move (alternatives: random k opt move / number of swap or interchange moves) 
            def perturbation(solu): 
                sliceLength = int(len(solu)/4)
                p1 = 1 + random.randrange(0,sliceLength) 
                p2 = p1 + 1 + random.randrange(0,sliceLength) #Combine first and fourth slice in order
                p3 = p2 + 1 + random.randrange(0,sliceLength) #Combine third and second slice in order          
                perturbation = solu[0:p1] + solu[p3:] + solu[p2:p3] + solu[p1:p2] #Return the combination of the above two combined slices
                return perturbation
            
            #Acceptance criterion
            def acceptance_criterion(solu, test_solu, not_accepted):
                best_solu = solu
                if cost(test_solu) <= cost(solu):
                    best_solu = copy.deepcopy(test_solu)
                    not_accepted = 0
                else:
                    best_solu = copy.deepcopy(solu)
                    not_accepted += 1
                
                return best_solu, not_accepted
            
            #Generate restart solution
            def generate_restart_solution(o):
                restart_solution = random.sample(o, len(o)) 
                return restart_solution
            
            #Iterated local search algorithm     
            def iterated_local_search(initial_solution, count_max, not_accepted_max):
                       
                #Step 1: Introduce an initial_solution
                best_solution_ILS = initial_solution
                
                #Step2: Local search on initial solution
                solution = local_search(best_solution_ILS)
                    
                count = 0
                not_accepted = 0 
                
                while not count == count_max:          
                    
                    #Step 3: Perturbation
                    sol = perturbation(solution)
                    #Step 4: Local search on perturbation
                    test_sol = local_search(sol)
                    
                    #Step 5: Compare local search + local search on perturbation
                    best_solu = acceptance_criterion(solution, test_sol, not_accepted)
                    not_accepted = best_solu[1]        
                    if not_accepted <= not_accepted_max:
                        solution = copy.deepcopy(best_solu[0])
                        not_accepted = best_solu[1]            
                        count += 1
                
                    else: #Step 6: Search history - Restart search if a number of iterations no improved solution is found
                        restart = generate_restart_solution(o)
                        solution = local_search(restart)
                        not_accepted = 0
                        count = 0
                        
                    #Store best solution of ILS algorithm
                    if cost(best_solu[0]) < cost(best_solution_ILS):
                        best_solution_ILS = copy.deepcopy(best_solu[0])        
                                           
                return best_solution_ILS
                    
            #OUTPUT
            solution_ILS = iterated_local_search(initial_solution, count_max, not_accepted_max)
            orders = solution_ILS                 
            
            order_1 = orders[:int(len(orders)/2)]
            order_2 = orders[int(len(orders)/2):] 
#            print(order_1)  
#            print(order_2)
            
            ##timinig
            wrapped = wrapper(iterated_local_search, initial_solution, count_max, not_accepted_max)
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
                            
            batched.to_csv(f+'_batched_2'+g+'_ILS_config_'+s+'.csv',  index=None, header=False) 
            
results = [picking_lines, measurements, configurations, initial_solutions, timing]

#print to csv
file = open('ILS_all_files_output_config_5_updated.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(results)            
    
print('\007')        
print('Metta')
                
                     
                
                
