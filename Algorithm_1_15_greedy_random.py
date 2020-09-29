# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 09:01:30 2018

@author: 20304269
"""

#20180821_greedy_rtb_assignment_all_files.py

#!!!TO DO: !!!change measurement + !!!-extension!!! + !!!choose search-logic!!! + !!!change batched_2-extension according to search-logic!!!

import pandas as pd
import numpy as np
import random
import time
from datetime import timedelta

filenames = [#'20130425_3150_LOLS_2' , 
             '20130227_1065_LOLS_2']#, '20130422_2964_LOLS_5', '20130403_2137_LOLS_2', '20130403_2147_LOLS_2', \
#             '20130422_2968_LOLS_5', '20130320_1756_LOLS_2', '20130209_521_LOLS_2', '20130222_852_LOLS_2', '20130223_1814_LOLS_5', \
#             '20130424_3142_LOLS_8', '20130406_2459_LOLS_2', '20130322_1810_LOLS_2', '20130326_1865_LOLS_2', '20130410_2913_LOMS_2', \
#             '20130322_1813_LOMS_8', '20130327_1966_LOMS_2', '20130307_1503_LOMS_5', '20130207_505_LOMS_2', '20130326_1868_LOMS_5', \
#             '20130225_926_LOMS_2', '20130409_2503_LOMS_2', '20130411_2569_LOMS_2', '20130312_1569_LOMS_3', '20130418_2817_LOMS_8', \
#             '20130307_1460_LOMS_8', '20130219_779_LOSS_2', '20130307_1501_LOSS_2', '20130228_1079_LOSS_8', '20130228_1080_LOSS_5', \
#             '20130409_2502_LOSS_5', '20130410_2550_LOSS_8', '20130415_2640_LOSS_5', '20130403_2037_LOSS_3', '20130313_1605_LOSS_2', \
#             '20130318_1715_LOSS_5', '20130216_704_LOSS_2', '20130326_1942_LOSS_5', '20130402_2022_MONS_2', '20130216_701_MONS_5', \
#             '20130212_591_MONS_2', '20130220_790_MONS_5', '20130212_592_MONS_2', '20130225_910_MONS_2', '20130416_2710_SOLS_6', \
#             '20130214_669_SOMS_7', '20130208_517_SOMS_6', '20130223_867_SOSS_5', '20130208_515_SOSS_5', '20130425_3160_SOLS_6']

for f in filenames:
#    print('picking line name:', f)  
    
    ##INPUT
    df = pd.read_csv(f+'_stop_ratio.csv', header=None) #TODO: change measurement!!!
    #print('picking line measurement:', df.head(3))

    #change same row/column to dummy entry 1000
    #https://stackoverflow.com/questions/38794169/replace-values-in-dataframe-here-rowname-is-equals-to-column-name
    for col in df:
        df.loc[col, col] = 1000 
    
    #print(df)
    #print(df.dtypes)
    #print(df.index)
    #print(df.column)       
           
    ##different search-logics:
    #top-down
    o = df[0].values.tolist()[1:] #second row tolist including all , slice off first dummy entry 1000
    #print('o', o)
    #bottom-up
    o_r = o[::-1] #[start:stop:step]
    #print('o_r', o_r)
    #random
    o_rand = random.sample(o, len(o))
#    print('o_rand', o_rand)
    
    m = df.values #get a numpy array from df
    #print('m', m)
    
    #empty lists for batching
    order_1 = []
    order_2 = []

    #INPUT for batching
    df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
#    print('picking line data:', df2.head(3))
    
    branch_list = df2['BRANCH_NO'].tolist()
    #print(branch_list)
#    print('branch+list: ', len(branch_list))
    
    batch_list = []
   
    ##timing
    start_time = time.time()
    
    ###greedy algorithm   
    for n in o_rand: #TODO !!!choose search-logic!!!
    #    print('n in logic:', n)
        for row in m:
                if row[0] == n and np.isin(n, order_2, invert=True): #choose row according to search-logic AND only choose n's that have not been used in order_2 yet
    #                print('n after checks:', n)          
                    order_1.append(n)
    #                print('row:', row)     
                    choose = min(row)
    #                print('choice:', choose)
                    loc_r = np.where(m[:,0] == n)[0] #row locations in length-1 array
                    loc_c = np.where(row == choose)[0] #column locations in length-1 array
    #                print('loc_r:', loc_r)
    #                print('loc_c:', loc_c)
                    r = int(loc_r[0]) #!choosing the first entry does not always result in the best result but is fast
                    c = int(loc_c[0])
    #                print('r:', r)
    #                print('c:', c)
                    n_r = m[r] #row which name will be added to order_1 - similar to n
    #                print('n_r:', n_r)
    #                print('n_r_loc:', n_r[0])
                    n_c = m[c]
    #                print('n_c:', m[c])
    #                print('n_c_loc:', n_c[0])
                    order_2.append(n_c[0]) #column name that will be added to order_2
    #                print('element:', m[r][c]) #delete all rows/columns "connected" to this element at once
                    m = np.delete(m, [r,c], 0) 
                    m = np.delete(m, [r,c], 1)
    #                print('new m:', m)  

    ##timing   
    elapsed_time_secs = time.time() - start_time
    msg = "Execution took: %s secs (Wall clock time)" % timedelta(seconds=round(elapsed_time_secs))
    #print(msg)
    
    #---------------------------------------------
    
#    print('order_1: ', order_1)
#    print('order_2: ', order_2)
#    print('order_1s length:' , len(order_1))
#    print('order_2s length:' , len(order_2))     
   
    
    ###order batching (size of 2)
    for y in range(len(branch_list)):
        for x in range(len(order_1)):
            if branch_list[y] == order_1[x]:
                batch_list.append('Batch {}'.format(x+1))
            elif branch_list[y] == order_2[x]:
                batch_list.append('Batch {}'.format(x+1))
     
    #print(batch_list)
    #print(len(batch_list))
    
    ##Third column with 'BATCH_NO'
    df2['BATCH_NO'] = batch_list 
#    print('df2:', df2.tail)
      
    #Group by batch and sum up entries
    batched = df2.groupby(['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO','BATCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE'], as_index=False).agg \
     ({'BRANCH_NO' : 'count', \
      'NUM_UNITS_y' : 'sum', \
      'NUM_BRANCHES' : 'sum', \
      'NUM_UNITS_x' : 'sum', \
      'VOLUME_PER_UNIT' : 'sum',\
      'WEIGHT_PER_UNIT_KG' : 'sum'})
    
    #print to txt-file 
    file = open(f+'_time_span_ratio_o.txt', 'w') #TODO !!!change batched_2-extension!!!
    file.writelines('Picking line: ')
    file.writelines(f)
    file.writelines('\n')
    file.writelines(msg)
    file.close() 
    
#    #OUTPUT: order_1 and order_2 from greedy algorithm
#    print('picking line name:', f)
#    print('order_1:', order_1)
#    print('order_2:', order_2)
    
#    #OUTPUT: batches of size 2
#    print('picking line name:', f)
#    print(batched) 
        
    batched.to_csv(f+'_batched_2_stop_ratio_o_rand.csv',  index=None, header=False) #TODO !!!change batched_2-extension!!!
    
print('\007')