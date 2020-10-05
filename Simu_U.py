# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 15:15:15 2019

@author: 20304269

"""
#source:20190917_simu_u_line_modul_10_config_congestion.py

import pandas as pd
import numpy as np
import csv

##TIMES (Video May 2019 - 20190530_3554_u_m_10_cycles_no_break.xlsx 'Analysis') #higher number in pick density
#walk
#np.random.triangular(1.154786739,1.546958292,1.962562591)
#pick
#np.random.triangular(3.502216603,4.4525246,5.155583882)
#prep+pack
#np.random.triangular(18.94534861,23.75035999,32.56450258)

##lower number in pick density
#walk
#np.random.triangular(1.254787,1.5700504,1.722412303)
#pick
#np.random.triangular(3.502216603,4.577524542,6.019965)
#prep+pack
#np.random.triangular(21.29224608,24.43679019,32.56450258)

##function that prints max SKUs
def sort_max_SKU(df):
    groups = df.groupby(['SKU_NO']).BATCH_NO.nunique()
    sorted_groups = groups.reset_index('SKU_NO').sort_values(by=['BATCH_NO'], ascending=False)
    sorted_groups = sorted_groups.reset_index(drop=True)
    return sorted_groups

#output for all files
filename = []
extension = []
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
picking_bays_order = []
packing_boxes_order = []
walking_speed_order = []
picking_speed_order = []
packing_speed_order = []     

filenames = ['L2U_picks_3519']#, 'L2U_picks_3357', 'L3U_picks_3378', 'L3U_picks_3600', 'L3U_picks_3662', 'L3U_picks_3554', 'L3U_picks_3520', 'L3U_picks_3317', 'L3U_picks_3516', 'L2U_picks_3369']

for f in filenames:       
  
#    extensions = ['_sim']
    #all files have been run on a nearest end heuristic
#    extensions = ['_sim_batched_2_sim_random_FIFO']
#    extensions = ['_sim_batched_2_sim_stop_ratio_o_rand']
#    extensions = ['_sim_batched_2_sim_span_ratio_orders_smallest_entry'] 
    extensions = ['_sim_batched_2_sim_span_stops_orders_smallest_entry']   
                  
    for e in extensions:     
      
        ##Iterate through split lists 
   
        if f == 'L2U_picks_3519' or f == 'L3U_picks_3378' or f == 'L3U_picks_3600' or f == 'L3U_picks_3520' or f == 'L2U_picks_3369':
            #picking line information - half line (1)
            line = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176]
            last_bay = 176
            line_length = 76
            
        else:
            #picking line information - half line (2)
            line = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164]
            last_bay = 164    
            line_length = 64
           
        print('picking_line', f)
        print('length of line', last_bay)
        
        for k in range(8,9):
            
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
                    if f == 'L3U_picks_3516' or f == 'L2U_picks_3369' or f == 'L3U_picks_3520':
                        m = np.random.triangular(18.94534861,23.75035999,32.56450258) #small data set
                    else:
                        m = np.random.triangular(21.29224608,24.43679019,32.56450258)
                    
                    pack_speed.append(m)
        #            print('pack_speed in for', pack_speed)
                    
        #        print('pack_speed', pack_speed)
                packing = sum(pack_speed)
        #        print('packing', i, packing)
                
#                packing_boxes.append(pack_slip)
#                packing_speed.append(packing)         
                                                     
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
            #        print('start_bay', start_bay)
            
                    if stops[0] <= stops[-1] and start_bay <= stops[0]:
                        span = line[line.index(start_bay):line.index(stops[-1])+1] #walked span per order
#                        print('span_if', span)
                      
                    else: 
                        span = line[line.index(start_bay):line.index(last_bay)+1] + line[line.index(first_bay):line.index(stops[-1])+1] #walked span per order around conveyor
#                        print('span_else', span)
                    
                    ## start picking from last location DIRECTLY
                    if span[-1] == last_bay: #if last location is last location in cycle
                        start_bay = first_bay
                    else:            
                        start_bay = span[-1] 
                    
#                    print('span_changing_start', span)
                    
                    #########################################
                    
                    walk_bays = []
                    walk_back = []
                    walk_speed = []
                      
                    for j in span:            
                        walk_bays.append(j)
            #            print('walkbays', walk_bays)
                    if orders[-1] == i: #walk back to beginning for last order
            
                        for w in [b for b in range(span[-1],last_bay+1) if b != span[-1]]:
                            walk_back.append(w)
            #            print('walk_back', walk_back)
                        
                        walk_bays.extend(walk_back)
            #            print('walk_bays_extended', walk_bays)                        
                    
                    for k in walk_bays:
                        if f == 'L3U_picks_3516' or f == 'L2U_picks_3369' or f == 'L3U_picks_3520':
                            k = np.random.triangular(1.154786739,1.546958292,1.962562591) #small data set
                        else:
                            k = np.random.triangular(1.254787,1.5700504,1.962562591)
                        
                        walk_speed.append(k)
                            
            #        print('walk_bays', walk_bays)            
                    walking = sum(walk_speed)
            #        print('length of walk bays', len(walk_bays))             
                    
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
                        if f == 'L3U_picks_3516' or f == 'L2U_picks_3369' or f == 'L3U_picks_3520':
                            l = np.random.triangular(3.502216603,4.4525246,5.155583882) #small data set
                        else:
                            l = np.random.triangular(3.502216603,4.577524542,6.019965)
                        
                        pick_speed.append(l)       
            
            #        print('length of pick bays', len(pick_bays))  
            #        print('length of pick_speed', len(pick_speed))
            #        print('pick_speed', pick_speed)
                    picking = sum(pick_speed)            
            #        print('picking', i, picking)
            
                    df2 = pd.read_csv('20200312_simu_u_line_cycle_picker_no_module.csv', names =['filename_delay', 'name_extension', 'cycle_no', 'picker_no', 'occupied'])
#                               
                    ###time delay
                    delay_time = []
                    
#                    print(df2.head())
                    
                    delay = df2.loc[(df2['filename_delay'] == f) & (df2['name_extension'] == e) & (df2['picker_no'] == 8)]['occupied'].values[0] ##https://stackoverflow.com/questions/22546425/how-to-implement-a-boolean-search-with-multiple-columns-in-pandas
#                                    
                    if not delay == False:
                        for d in range(delay):
                            ##TODO: Change between no batch and batch                            
#                            d = np.random.triangular(0.875554151,1.002155147,1.144381136) #no batching
                            d = np.random.triangular(1.751108302,2.004310294,2.288762271) #batching algo 2
                            delay_time.append(d)
                    
                    print('len delay time', len(delay_time))
                    delaying = (np.sum(delay_time))
                    print('delaying', delaying)
                                    
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
                    picking_bays_order.append(len(pick_bays))
                    walking_speed_order.append(walking)
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
                             
                                              
#        ###pick density M
#        maximal_SKU = sort_max_SKU(df).BATCH_NO[0]
#        print(len(all_stops), line_length, maximal_SKU)
#        picking_density_m = len(all_stops) / (line_length * maximal_SKU)        
#        print('picking_density_m', picking_density_m)
#            
#        #all configurations for data                    
#        filename.append(f)
#        extension.append(e)            
#        picker_no.append(pickers)
#        pick_dens_m.append(picking_density_m)
        avg_times_total = np.mean(total_times)
        avg_total_times.append(avg_times_total)
        print('avg_total_times', avg_total_times)
        avg_times_total_delaying = np.mean(total_times_delaying)
        avg_total_time_delaying.append(avg_times_total_delaying) #list for the data
        print('avg_total_time_delaying', avg_total_time_delaying)
#        avg_times_total_pickers = np.mean(total_times_pickers)
#        avg_total_time_pickers.append(avg_times_total_pickers) #list for the data
#        print('avg_total_time_pickers', avg_total_time_pickers)
        
data = [filename_order, extension_order, configuration_order, order_name, walking_bays_order, picking_bays_order, packing_boxes_order, walking_speed_order, picking_speed_order, packing_speed_order]

#print to csv 
##order specific
file = open('20200312_simu_u_line_module_nearest_end_delay_8_pickers'+e+'_10_config_order_specific.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(data)       
            
                                               
data2 = [filename, extension, picker_no, pick_dens_m, avg_total_times, avg_total_time_delaying, avg_total_time_pickers]
        
##batching algorithm specific   
file = open('20200312_simu_u_line_module_nearest_end_delay_8_pickers'+e+'_10_config.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(data2)                     
            
print('\007')        
print('Metta!')