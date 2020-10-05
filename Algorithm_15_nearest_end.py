# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 08:02:29 2018

@author: 20304269
"""

#TODO: 20200310_nearest_end_tested_all_files_56_SKUs_u.py

import pandas as pd

#-----------

picking_lines = []
order_seq = []
#cycles_traversed = []

#-----------

#TODO: input filenames
filenames = ['4881+4887+4856+4884']

for e in filenames:  
    
        #no batch
    extension_names = ['']       
    
#    extension_names = ['_batched_2_sim_stop_ratio_o_rand']
#    extension_names = ['_batched_2_sim_span_stops_u_line_orders_smallest_entry']
    
    for g in extension_names:
        
        scenarios = ['Scen_3_']
    
        for c in scenarios:

            lines = ['df1', 'df2', 'df3', 'df4']
            
            for o in lines: 
                
                assignment = ['_1_1', '_1_2','_1_3', '_1_4', '_1_5']
                
                for a in assignment:
    
                    extension = ['_SKU_location_random_', '_SKU_location_freq_']
                         
                    for z in extension:
                        
                        #TODO: change between no batch and batch
                        
                        df = pd.read_csv(e+z+c+o+a+'.csv', names =['LINE_NO', 'DBN_NO', 'BATCH_NO', 'LOCATION_CODE', 'SKU_NO', 'REQUIRED_QTY', 'PACK_SLIP_CODE']) #no batch
                        
#                        df = pd.read_csv(e+z+c+o+a+g+'.csv', names =['LINE_NO', 'BATCH_NO', 'LOCATION_CODE', 'SKU_NO', 'DBN_NO', 'BRANCH_COUNT', 'REQUIRED_QTY', 'PACK_SLIP_CODE']) #batched algo
                        print(df.head())
                        
                        #1: group orders
                        df.sort_values(['BATCH_NO','LOCATION_CODE'], inplace = True)
                        orders = df.groupby(['BATCH_NO'], as_index = False) #must be sorted according to locations!                                             
                                  
                        ##Iterate through split lists                     
                        ##after batching specific!           
                        #0: picking line information 176 1floor+xA
                        if (e == '4881+4887+4856+4884' and o == 'df1') or (e == '4881+4887+4856+4884' and o == 'df4' and a == '_1_1') or (e == '4783+4774+4784+4786' and o == 'df1') or (e == '4824+4812+4804+4825' and o == 'df1') or (e == '4824+4812+4804+4825' and o == 'df4' and a == '_1_2'):  
                             #picking line information - half line (1)
                            line = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176]
                            last_bay = 176
                            first_bay = 101
                            bay_max = 76
                            
    #                        #picking line information - 168 2floor                        
    #                    elif (e == '4840+4850+4854' and o == 'df1') or (e == '4903+4902+4888' and o == 'df1') or (e == '4771+4774+4773' and o == 'df1') or (e == '4793+4798+4804' and o == 'df1'):                       
    #                        line = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168]
    #                        last_bay = 168 
    #                        first_bay = 101
    #                        bay_max = 68
                            
                            #picking line information - 164 xB                     
                        else:
                            line = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164]
                            last_bay = 164 
                            first_bay = 101
                            bay_max = 64                     
                          
                        print('picking_line', e+z+c+o+a+g)
                        print('length of line', bay_max)      
                        
                        #3: for each order make a third column with distance
                        dist_list = []
                        for index, row in orders: 
                            
                            item = 0
                            for x in row.get('LOCATION_CODE').values:       
                             
                #                print(item)
                                full = row.get('LOCATION_CODE').values[item-1] - row.get('LOCATION_CODE').values[item] #distance between last item of order and first 
                #                print('full', full)
                                if full < 0: #check if walked over starting point
                                    dist_list.append(full + bay_max) #if yes, add bay_max to distance
                                else: 
                                    dist_list.append(full) #otherwise add distance
                                item +=1 
                    #            print(item)
                            
                #        print(dist_list)
                        
                        #Append list as third column
                        df['END_LOCATION'] = dist_list #adds list's values to dataframe
                        
                        #print(list(orders))
                        
                        #ALGORITHM: Determine sequence of orders
                        sequence = [] 
                        count = 0
                        curr_loc = 101
                        while True:
                            
                            if not len(df.index) == 0:         

                                locations = df['LOCATION_CODE'].tolist()
                #                print('Updated locations:', locations) 
                #                print('curr_loc before condition:', curr_loc)
                                if curr_loc in locations:
                #                    print('curr_loc start', curr_loc)
                                    list_1 = df.loc[df['LOCATION_CODE'] == curr_loc, 'END_LOCATION'].tolist() #IMPORTANT: https://stackoverflow.com/questions/36684013/extract-column-value-based-on-another-column-pandas-dataframe      
                #                    print('END_LOCATION to compare', list_1)
                                    h = min(list_1) # find the smallest END_LOCATION
                        #            print('Smallest END_LOCATION', a)
                                    seq_all = df.loc[(df['LOCATION_CODE'] == curr_loc) & (df['END_LOCATION'] == h), 'BATCH_NO'].tolist() #multiple conditions TODO change to item
                                    seq = seq_all[0]
                                    sequence.append(seq)
                                    df = df.set_index('BATCH_NO').drop(seq).reset_index() #drop row if it contains variable; https://stackoverflow.com/questions/46655712/remove-rows-and-valueerror-arrays-were-different-lengths?rq=1
                                    b = curr_loc + h # determine end location !no additional +1!

                                    if b > last_bay: 
                                        curr_loc = b - bay_max
                                        count  +=1   
                                    else: 
                                        curr_loc = b  
                             
                                else:
                                    d = curr_loc + 1 # determine end location
                                    if d > last_bay:
                                        curr_loc = d - bay_max
                                        count +=1
                                    else:
                                        curr_loc = d
                                        
                            else:
                                if curr_loc != first_bay and curr_loc < last_bay: #finishes the cycle
                                    count +=1
                                break
                        
                    #            print('---')
                        picking_lines.append(e)
                        order_seq.append(sequence)    
                #        cycles_traversed.append(count) not all counts included as sometimes jump ober max     
                #            print('---')   
                        
                        #Group by batch and sum up entries
                        print('sequence', sequence)
                        print('len sequence', len(sequence))
                #        print('cycles traversed', cycles_traversed)
                
                        #TODO: change between no batch and batch
                        
                        df2 = pd.read_csv(e+z+c+o+a+'.csv', names =['LINE_NO', 'DBN_NO', 'BATCH_NO', 'LOCATION_CODE', 'SKU_NO', 'REQUIRED_QTY', 'PACK_SLIP_CODE']) #no batch
                                        
#                        df2 = pd.read_csv(e+z+c+o+a+g+'.csv', names =['LINE_NO', 'BATCH_NO', 'LOCATION_CODE', 'SKU_NO', 'DBN_NO', 'BRANCH_COUNT', 'REQUIRED_QTY', 'PACK_SLIP_CODE']) #batched algo
#                        print(df2.head())
#                        print(df2.dtypes())
#                
                        df2['BATCH_NO_cat'] = pd.Categorical(df2['BATCH_NO'], categories=sequence, ordered=True)
                #        print(df2.head())
                        df2.sort_values(['BATCH_NO_cat','LOCATION_CODE'], inplace=True)
                        df2 = df2.iloc[:, :-1]
                       
                    #    #OUTPUT:       
                        df2.to_csv(e+z+c+o+a+g+'_sim.csv',  index=None, header=False)  
                       
print('\007')
print('Metta!')               
        