# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 13:45:44 2018

@author: 20304269
"""

#source:20180920_smallest_entry_search_logic_all_files_final_output.py

import pandas as pd
import numpy as np
import time
import csv

picking_lines = []
measurements = []
search_logic_name = []
order1 = []
order2 = []
timing = []

#TODO: input filenames
file_names = ['20130425_3150_LOLS_2']

for f in file_names:
    
    extension_names = ['_stops' , '_nonidentical_stops' , '_orders_serviced_bays', '_min_span', '_nonidentical_span', '_orders_serviced_spans', '_span_ratio']
    for g in extension_names:
    
        #INPUT for greedy algorithm
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
            
        #input for algorithm  
        order_1 = []
        order_2 = []
        
        #input for algorithm
        stop = int(len(df.index)/2)
        #print('stop: ', stop)              
                      
        ##timing
        start_time = time.time()  
        
        ###greedy algorithm
        while not len(order_2) == stop:
               
            #smallest entry:
            mini = df.min(0).tolist()
            mini_idx = df.idxmin(0).tolist()
            if len(order_2) == stop-1:
                order_1.append(mini_idx[1])
                order_2.append(mini_idx[0])
            else:     
                #second smallest entry: 
                #https://stackoverflow.com/questions/46284970/python-dataframe-add-a-column-and-insert-the-nth-smallest-value-in-the-row
                #option 1: fastest for large number of columns!
                nth_p = np.partition(df.values, 2, axis=1)[:, :2].max(1).tolist()
                nth_idx = np.argpartition(df.values, 2, axis=1)[:, :2].max(1).tolist()
                columns = df.columns.tolist()
                nth_p_idx = [columns[k] for k in nth_idx]
                
                #create additional columns
                df['mini'] = mini
                df['mini_idx'] = mini_idx
            
                df['nth_p'] = nth_p
                df['nth_p_idx'] = nth_p_idx
                df['diff'] = df['nth_p'] - df['mini']
                
                sel = np.max(df['diff'])
                sel_row = np.argmax(df['diff'])
                sel_col = df.loc[sel_row]['mini_idx']
                order_1.append(sel_row)
                order_2.append(sel_col)
                df.drop([sel_row, sel_col] , axis=0, inplace=True)
                df.drop([sel_row, sel_col] , axis=1, inplace=True)
                df.drop(['mini', 'mini_idx', 'nth_p', 'nth_p_idx', 'diff'] , axis=1, inplace=True)
                #print('order_1_loop: ', order_1) 
                #print('order_2_loop: ', order_2)  
                     
        ##timing   
        elapsed_time_secs = time.time() - start_time
       
        #---------------------------------------------
                
        picking_lines.append(f)
        measurements.append(g)
        search_logic_name.append('smallest_entry_search')
        order1.append(order_1)
        order2.append(order_2)
        timing.append(elapsed_time_secs)              
                     
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
            
        batched.to_csv(f+'_batched_2'+g+'_orders_smallest_entry.csv',  index=None, header=False)
        
data = [picking_lines, measurements, search_logic_name, order1, order2, timing]
#print(data)

#print to csv
file = open('smallest_entry_search_all_files_output.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(data)
        
print('\007')
print('Metta')