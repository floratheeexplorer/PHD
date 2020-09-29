# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 13:45:44 2018

@author: 20304269
"""

#20180920_smallest_entry_search_logic_all_files_final_output.py

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

file_names = ['20130425_3150_LOLS_2' , '20130227_1065_LOLS_2', '20130422_2964_LOLS_5', '20130403_2137_LOLS_2', '20130403_2147_LOLS_2', \
             '20130422_2968_LOLS_5', '20130320_1756_LOLS_2', '20130209_521_LOLS_2', '20130222_852_LOLS_2', '20130223_1814_LOLS_5', \
             '20130424_3142_LOLS_8', '20130406_2459_LOLS_2', '20130322_1810_LOLS_2', '20130326_1865_LOLS_2', '20130410_2913_LOMS_2', \
             '20130322_1813_LOMS_8', '20130327_1966_LOMS_2', '20130307_1503_LOMS_5', '20130207_505_LOMS_2', '20130326_1868_LOMS_5', \
             '20130225_926_LOMS_2', '20130409_2503_LOMS_2', '20130411_2569_LOMS_2', '20130312_1569_LOMS_3', '20130418_2817_LOMS_8', \
             '20130307_1460_LOMS_8', '20130219_779_LOSS_2', '20130307_1501_LOSS_2', '20130228_1079_LOSS_8', '20130228_1080_LOSS_5', \
             '20130409_2502_LOSS_5', '20130410_2550_LOSS_8', '20130415_2640_LOSS_5', '20130403_2037_LOSS_3', '20130313_1605_LOSS_2', \
             '20130318_1715_LOSS_5', '20130216_704_LOSS_2', '20130326_1942_LOSS_5', '20130402_2022_MONS_2', '20130216_701_MONS_5', \
             '20130212_591_MONS_2', '20130220_790_MONS_5', '20130212_592_MONS_2', '20130225_910_MONS_2', '20130416_2710_SOLS_6', \
             '20130214_669_SOMS_7', '20130208_517_SOMS_6', '20130223_867_SOSS_5', '20130208_515_SOSS_5', '20130425_3160_SOLS_6']

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
    #        print('mini: ', mini)
            mini_idx = df.idxmin(0).tolist()
    #        print('mini_idx: ', mini_idx)
            if len(order_2) == stop-1:
                order_1.append(mini_idx[1])
                order_2.append(mini_idx[0])
            else:     
                #print(df.values) #make sure to still work on original matrix values!
                #second smallest entry: 
                #https://stackoverflow.com/questions/46284970/python-dataframe-add-a-column-and-insert-the-nth-smallest-value-in-the-row
                #option 1: fastest for large number of columns!
                nth_p = np.partition(df.values, 2, axis=1)[:, :2].max(1).tolist()
    #            print('nth_p: ', nth_p)
                nth_idx = np.argpartition(df.values, 2, axis=1)[:, :2].max(1).tolist()
    #            print('nth_idx: ', nth_idx)
                columns = df.columns.tolist()
    #            print('columns: ', columns)
                nth_p_idx = [columns[k] for k in nth_idx]
    #            print('nth_p_idx: ', nth_p_idx)
                
                #create additional columns
                df['mini'] = mini
                df['mini_idx'] = mini_idx
            
                df['nth_p'] = nth_p
                df['nth_p_idx'] = nth_p_idx
                df['diff'] = df['nth_p'] - df['mini']
    #            print(df)
                
                sel = np.max(df['diff'])
    #            print('sel: ',sel)    
                sel_row = np.argmax(df['diff'])
    #            print(sel_row)
                sel_col = df.loc[sel_row]['mini_idx']
    #            print(sel_col)
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
    #    print('df2:', df2.head(3))
          
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
print('YAS')