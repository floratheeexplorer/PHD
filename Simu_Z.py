# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 15:15:15 2019

@author: 20304269

"""
#20190709_simu_z_modul_10_config.py

import pandas as pd
import numpy as np
import csv

##TIMES (Video May 2019 - 20190530_3441_z_10_cycles_no_break.xlsx 'Analysis') #fast
#walk
#np.random.triangular(1.455870263,1.906861928,2.506904776)
#cross
#np.random.triangular(3.144826903,4.502265995,5.921426259)
#pick
#np.random.triangular(3.79836254,4.624397321,5.618632693)
#prep+pack
#np.random.triangular(13.92857308,21.28673885,31.92132938)

#number2 #big data set
#walk
#np.random.triangular(1.455870263,1.821141522,2.331873136)
#cross
#np.random.triangular(3.144826903,4.752265963,7.921426)
#pick
#np.random.triangular(3.262697,4.145940989,5.618632693)
#prep+pack
#np.random.triangular(17.40853524,24.03673847,35.92857)

#number !quicker!
#walk
#np.random.triangular(1.355870263,1.721141522,2.031873136)
#cross
#np.random.triangular(2.92265963,3.621426259,4.921426)
#pick
#np.random.triangular(3.262697,4.145940989,4.818632693)
#prep+pack
#np.random.triangular(14.92857308,21.28673885,24.92132938)

##function that prints max SKUs
def sort_max_SKU(df):
    groups = df.groupby(['SKU_NO']).BATCH_NO.nunique()
    sorted_groups = groups.reset_index('SKU_NO').sort_values(by=['BATCH_NO'], ascending=False)
    sorted_groups = sorted_groups.reset_index(drop=True)
    return sorted_groups

#output for all files
filename = []
name_extension = []
sorting = []
frame = []
location = []
picker_no = []
pick_dens_m = []
avg_total_times = []
avg_total_time_delaying = []
avg_total_time_pickers = []

first_bay = 101 

##order specific info
filename_order = []
extension_order = []
configuration_order = []
order_name = []
walking_bays_order = []
crossing_bays_order = []
picking_bays_order = []
packing_boxes_order = []
walking_speed_order = []
crossing_speed_order = []
picking_speed_order = []
packing_speed_order = []        
      
filenames = ['L3Z_picks_3660', 'L3Z_picks_3410','L3Z_picks_3141', 'L3Z_picks_3569', 'L3Z_picks_3281', 'L3Z_picks_3735', 'L3Z_picks_3597', 'L3Z_picks_3441', 'L3Z_picks_3709', 'L3Z_picks_3348']

for f in filenames:       
    
#    extensions = ['_sim']
    #all files have been run on a nearest end heuristic
    extensions = ['_sim_batched_2_sim_random_FIFO']
#    extensions = ['_sim_batched_2_sim_stop_ratio_o_rand']
#    extensions = ['_sim_batched_2_sim_span_ratio_orders_smallest_entry'] 
#    extensions = ['_sim_batched_2_sim_span_stops_orders_smallest_entry']   
                 
    for e in extensions:         
          
        if f == 'L3Z_picks_3660' or f == 'L3Z_picks_3141' or f == 'L3Z_picks_3569' or f == 'L3Z_picks_3281' or f == 'L3Z_picks_3709':
            #picking line information - half line (1)
            line_length = 72
            last_bay = 172
            
        else:
            #picking line information - half line (2)
            line_length = 60
            last_bay = 160   
           
        print('picking_line', f)
        print('length of line', last_bay)
      
        for k in range(8,9): #8 pickers in the module (can be changed)
            
            pickers = k
            
            #walking_bays = []
            #picking_bays = []
            packing_boxes = []
            walking_speed = []
            picking_speed = []
            packing_speed = [] 
            
            configuration = []
            total_times = [] #to generate average of configurations
            total_times_delaying = [] #to generate average of configurations
            total_times_pickers = [] #to generate average of configurations 
            
            con = list(range(1,2)) #10 configurations per picking line
            config = [str(l) for l in con]
            
            for s in config:                 
                 
                ##TODO: Change between no batch and batch                               
#                df = pd.read_csv(f+e+'.csv', names =['LINE_NO', 'BATCH_NO_HIST', 'BATCH_NO', 'LOCATION_CODE', 'SKU_NO', 'REQUIRED_QTY', 'PACK_SLIP_CODE']) #no_batch
                df = pd.read_csv(f+e+'.csv', names =['LINE_NO', 'BATCH_NO', 'LOCATION_CODE', 'SKU_NO', 'BRANCH_NO_HIST', 'BRANCH_COUNT', 'REQUIRED_QTY', 'PACK_SLIP_CODE']) #batched algo
                
                time_for_all_orders = []
                delaying_speed = []               
                
                ### time to pack            
                pack_speed = []
                
                pack_slip_number = df['PACK_SLIP_CODE'].tolist()
                pack_slip = pack_slip_number[0] 
                
                for m in np.unique(pack_slip):
                    if f == 'L3Z_picks_3709' or f == 'L3Z_picks_3348':
                        m = np.random.triangular(13.92857308,21.28673885,31.92132938)
                    elif f == 'L3Z_picks_3735' or f == 'L3Z_picks_3597':
                        m = np.random.triangular(14.92857308,21.28673885,24.92132938)
                    else:
                        m = np.random.triangular(13.92857308,21.28673885,27.92132938)
                        
                    pack_speed.append(m)
        #            print('pack_speed in for', pack_speed)
                    
        #        print('pack_speed', pack_speed)
                packing = sum(pack_speed)
#                print('packing', packing)  
                
                packing_boxes.append(pack_slip)
                packing_speed.append(packing) 
                                 
                              
                #### walk + pick
                orders = df['BATCH_NO'].unique().tolist()
        #        print('orders', orders) 
                 
                all_stops = []                      
                start_bay = 101
                
                for i in orders:                      
                  
#                    print('this is order', i)
                    
                    stops = df.loc[(df['BATCH_NO'] == i), 'LOCATION_CODE'].tolist()
                    all_stops.extend(stops)
#                    print('stops', stops)                 
                                        
                    ### time to walk   
                   
                    ##Z picking!
                    
                    if stops[0] > stops[-1]:
                        stops.reverse()
                    #print('stops_storted', stops)
                    
                    bay_count = [4] #dummy for empties passed
                    cross_count = []
                    
                    for n in range(len(stops)-1): #iterates through list using index stopping at last order
                        if stops[n] % 2 != 0 and stops[n+1] % 2 != 0: #both stops are uneven
                            bay_count.append((stops[n+1] - stops[n]) / 2)
        #                    print('uneven')
#                            print('bay_count uneven', bay_count)
                        
                        elif stops[n] % 2 == 0 and stops[n+1] % 2 == 0: #both stops are even
                            bay_count.append(((stops[n+1] - stops[n]) / 2))
        #                    print('even')
#                            print('bay_count even', bay_count)
                            
                        else:
                            cross_count.append(1)
                            if stops[n+1] > stops[n]:
                                bay_count.append((((stops[n+1]+1) - stops[n]) / 2))
                        #        print('cross')
#                                print('bay_count cross if', bay_count)
                            else: 
                                bay_count.append((((stops[n]+1) - stops[n+1]) / 2))
                        #        print('cross')
#                                print('bay_count cross else', bay_count)
                    
                    if orders[-1] == i: #walk back to beginning from last order
                        bay_count.append(last_bay - stops[-1])
                    
                    passed_bays = int(sum(bay_count))                     
                  
                    ###########################################
                    
                    walk_bays = []
                    walk_speed = []
                    cross_speed = []
          
                    for j in range(passed_bays):           
                        walk_bays.append(j)          
                                    
                    for k in walk_bays:
                        if f == 'L3Z_picks_3709' or f == 'L3Z_picks_3348':
                            k = np.random.triangular(1.455870263,1.906861928,2.506904776)
                        elif f == 'L3Z_picks_3735' or f == 'L3Z_picks_3597':
                            k = np.random.triangular(1.355870263,1.721141522,2.031873136)
                        else:
                            k = np.random.triangular(1.455870263,1.821141522,2.331873136)
                        
                        walk_speed.append(k)
                            
                    #print('walk_bays', walk_bays)
                    #print('length of walk bays', len(walk_bays))  
                    
                    for c in cross_count:
                        if f == 'L3Z_picks_3709' or f == 'L3Z_picks_3348':
                            c = np.random.triangular(3.144826903,4.502265995,5.921426259)                    
                        elif f == 'L3Z_picks_3735' or f == 'L3Z_picks_3597':
                            c = np.random.triangular(2.92265963,3.621426259,4.921426)
                        else:
                            c = np.random.triangular(3.144826903,4.752265963,7.921426)
                            
                        cross_speed.append(c)
                    
                    #print('cross_count', cross_count)
                    #print('length of cross_count', len(cross_count))
                    
                    walk = sum(walk_speed)
                    cross = sum(cross_speed)
                    
                    walking = walk + cross
                    
#                    print('walking', walking)           
      
                                                                     
                    ### time to pick
                    
                    pick_bays = []
                    pick_speed = []
                    
                    for k in stops:
                        
                        ##TODO: Change between no batch and batch                          
#                        #no batching
#                        pick_bays.extend('p')
                        
                        #batching
                        if ((df['LOCATION_CODE'] == k) & (df['BATCH_NO'] == i) & (df['BRANCH_COUNT'] == 2)).any(): #count twice for picking two                                     
                            bays = ['p','p']
                            pick_bays.extend(bays)
                        else:   
                            pick_bays.extend('p')  
                      
                    for l in pick_bays: #every pick gets the same speed
                        if f == 'L3Z_picks_3709' or f == 'L3Z_picks_3348':                
                            l = np.random.triangular(3.79836254,4.624397321,5.618632693)
                        elif f == 'L3Z_picks_3735' or f == 'L3Z_picks_3597':    
                            l = np.random.triangular(3.262697,4.145940989,4.818632693)
                        else:
                            l = np.random.triangular(3.262697,4.145940989,5.618632693)
                            
                        pick_speed.append(l)       
            
            #        print('length of pick bays', len(pick_bays))  
            #        print('length of pick_speed', len(pick_speed))
            #        print('pick_speed', pick_speed)
                    picking = sum(pick_speed)            
#                    print('picking', picking)    
                    
                    df2 = pd.read_csv('20200312_simu_z_line_cycle_picker_no_line.csv', names =['filename_delay', 'name_extension', 'cycle_no', 'picker_no', 'occupied'])
#                               
                    ###time delay
                    delay_time = []
                    
#                    print(df2.head())
                    
                    delay = df2.loc[(df2['filename_delay'] == f) & (df2['name_extension'] == e) & (df2['picker_no'] == 8)]['occupied'].values[0] ##https://stackoverflow.com/questions/22546425/how-to-implement-a-boolean-search-with-multiple-columns-in-pandas
                    
                    if not delay == False:
                        for d in range(delay):
                            ##TODO: Change between no batch and batch                            
#                            d = np.random.triangular(0.81567425,1.09155372,1.404658173) #no batching
                            d = np.random.triangular(1.6313485,2.18310744,2.809316347) #batching algo 2
                            delay_time.append(d)
                    
#                    print('len delay time', len(delay_time))
                    delaying = (np.sum(delay_time))
#                    print(delaying)
                    
                    #time total                    
                    time_per_order = walking + picking
    #                    print('time_total', i, time_per_order)
                    time_for_all_orders.append(time_per_order)
                    
                    ##time total per order per file                   
                    filename_order.append(f)
#                    print('filename_order', len(filename_order))
                    extension_order.append(e)   
                    configuration_order.append(s)
                    order_name.append(i)   
#                    print('order_name', len(order_name))
                    walking_bays_order.append(len(walk_bays))
#                    print('walking_bays', len(walking_bays))
                    crossing_bays_order.append(len(cross_count))
                    picking_bays_order.append(len(pick_bays))
                    walking_speed_order.append(walking)
                    crossing_speed_order.append(cross)
                    picking_speed_order.append(picking)
                    
                    packing_boxes_order.append(pack_slip)
#                    print('packing_boxes', len(packing_boxes))
                    packing_speed_order.append(packing)                  
                                                                                                    
    #           ##times total for each configurations         
    #           configuration.append(s)  
#                print('configuration', configuration)
                times_total = np.sum(time_for_all_orders) + packing + delaying
                total_times.append(times_total)
                print('total_times', total_times)
                total_times_delaying.append(delaying)
                print('total_times_delaying', total_times_delaying)
                times_total_pickers = (np.sum(time_for_all_orders) + packing + delaying) / pickers 
                total_times_pickers.append(times_total_pickers)
                print('total_times_pickers', total_times_pickers)             
                                              
        ###pick density M
        maximal_SKU = sort_max_SKU(df).BATCH_NO[0]
        print(len(all_stops), line_length, maximal_SKU)
        picking_density_m = len(all_stops) / (line_length * maximal_SKU)        
        print('picking_density_m', picking_density_m)
            
        #all configurations for data                    
        filename.append(f)
        extension.append(e)            
        picker_no.append(pickers)
        pick_dens_m.append(picking_density_m)
        avg_times_total = np.mean(total_times)
        avg_total_times.append(avg_times_total)
        print('avg_total_times', avg_total_times)
        avg_times_total_delaying = np.mean(total_times_delaying)
        avg_total_time_delaying.append(avg_times_total_delaying) #list for the data
        print('avg_total_time_delaying', avg_total_time_delaying)
        avg_times_total_pickers = np.mean(total_times_pickers)
        avg_total_time_pickers.append(avg_times_total_pickers) #list for the data
        print('avg_total_time_pickers', avg_total_time_pickers)
                
data = [filename_order, extension_order, configuration_order, order_name, walking_bays_order, crossing_bays_order, picking_bays_order, packing_boxes_order, walking_speed_order, crossing_speed_order, picking_speed_order, packing_speed_order]

#print to csv 
##order specific
file = open('20200312_simu_z_line_module_nearest_end_delay_8_pickers'+e+'_10_config_order_specific.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(data)
                                       
data2 = [filename, extension, picker_no, pick_dens_m, avg_total_times, avg_total_time_delaying, avg_total_time_pickers]
        
##batching algorithm specific   
file = open('20200312_simu_z_line_module_nearest_end_delay_8_pickers'+e+'_10_config.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(data2)                     
            
print('\007')        
print('Metta!')