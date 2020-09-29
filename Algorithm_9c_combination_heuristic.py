# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 09:01:30 2018

@author: 20304269
"""

#20190328_greedy_stop_span_assignment_spans_example.py

import pandas as pd
import numpy as np
import random
import time
import csv

picking_lines = []
measurements = []
configurations = []
search_logic_name = []
order1 = []
order2 = []
timing = []

#0: picking line information
line = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156]
bay_max = 56
first_bay = 101
last_bay = 156

file_names = ['20130425_3150_LOLS_2' , '20130227_1065_LOLS_2', '20130422_2964_LOLS_5', '20130403_2137_LOLS_2', '20130403_2147_LOLS_2', \
         '20130422_2968_LOLS_5', '20130320_1756_LOLS_2', '20130209_521_LOLS_2', '20130222_852_LOLS_2', '20130223_1814_LOLS_5', \
         '20130424_3142_LOLS_8', '20130406_2459_LOLS_2', '20130322_1810_LOLS_2', '20130326_1865_LOLS_2', '20130410_2913_LOMS_2', \
         '20130322_1813_LOMS_8', '20130327_1966_LOMS_2', '20130307_1503_LOMS_5', '20130326_1868_LOMS_5', \
         '20130225_926_LOMS_2', '20130409_2503_LOMS_2', '20130411_2569_LOMS_2', '20130312_1569_LOMS_3', '20130418_2817_LOMS_8', \
         '20130228_1079_LOSS_8', '20130228_1080_LOSS_5', \
         '20130410_2550_LOSS_8', '20130403_2037_LOSS_3', \
         '20130216_704_LOSS_2', '20130326_1942_LOSS_5', '20130402_2022_MONS_2', '20130216_701_MONS_5', \
         '20130212_591_MONS_2', '20130220_790_MONS_5', '20130212_592_MONS_2', '20130225_910_MONS_2' ]

for f in file_names:
        
        
        extension_names = ['_min_span', '_nonidentical_span' , '_nonidentical_min_span', '_span_ratio', '_span_stops', '_non_stops_non_span' , '_ratio_add']
                
        for g in extension_names:            
                                 
                #INPUT for algorithm
            #   print('name of picking_line:', f+g)
                df = pd.read_csv(f+g+'.csv', header=None)
             
                for col in df:
                    df.loc[col, col] = 100 
                    
    #            print(df)
                    
                #INPUT for batching + search logic
                df2 = pd.read_csv(f+'.csv', names = ['SCHEDULE_DATE', 'RELEASE_DATE', 'LINE_NO', 'BRANCH_NO', 'DBN_NO', 'SKU_NO', 'LOCATION_CODE', 'NUM_UNITS_y', 'NUM_BRANCHES', 'NUM_UNITS_x', 'VOLUME_PER_UNIT', 'WEIGHT_PER_UNIT_KG']) 
                branch_list = df2['BRANCH_NO'].tolist()
                      
                ###different search-logics:
#               ##spans
                
                min_span = pd.read_csv(f+'.csv', usecols=[3,6], names = ['BRANCH_NO','LOCATION_CODE'])
                min_span['LOCATION_CODE'] = min_span['LOCATION_CODE'].str[1:]
                min_span['LOCATION_CODE'] = min_span['LOCATION_CODE'].astype(np.int64)
                #print(min_span)
                
                #2: group orders
                orders = min_span.groupby(['BRANCH_NO'], as_index=False)
                order_no = min_span[('BRANCH_NO')].unique() #get the BRANCH_NO to create a list of unique order numbers
            
                ###Measure "distance" -----------------------------------------------------------------------
                
                start_orders=[] #creates list with start of orders
                end_orders=[] #creates list with end of orders
                
                for index, row in orders:
                    gap=[]
                    for l in line:
                        gaps = row.get('LOCATION_CODE').values
                        if not l in gaps:
                            gap.append(l)
                            
                #            print('gap', gap)   
                        else:
                #            print('no')
                            l+=1
                    
                    ## determine the sequence of consecutive numbers        
                    #https://stackoverflow.com/questions/44392364/how-to-find-the-longest-consecutive-chain-of-numbers-in-an-array
                    longest_seq = max(np.split(gap, np.where(np.diff(gap) != 1)[0]+1), key=len).tolist()
                #    print('longest_seq:', longest_seq)
                    
                    ##determine the longest sequence around the conveyor belt
                    if longest_seq[-1] == last_bay:
                        if gap[0] == first_bay and gap[-1] == last_bay:
                            first_seq = (np.split(gap, np.where(np.diff(gap) != 1)[0]+1)[0]).tolist()
                #            print('first_seq:', first_seq)
                            longest_seq = longest_seq + first_seq
                #            print('longest_seq:', longest_seq)
                   
                    if longest_seq[0] == first_bay:
                        if gap[0] == first_bay and gap[-1] == last_bay:
                            last_seq = (np.split(gap, np.where(np.diff(gap) != 1)[0]+1)[-1]).tolist()
                #            print('last_seq:', last_seq)
                            longest_seq = last_seq + longest_seq
                #            print('longest_seq:', longest_seq)
                   
                    ## determine the starting and ending bay
                    if longest_seq[-1] + 1 > last_bay: #start from the beginning
                        start_order = first_bay
                #        print('start_order_1:', start_order)
                        start_orders.append(start_order)
                        if longest_seq[0] -1 < first_bay: # in case one needs to end at the ending
                            end_order = last_bay
                            end_orders.append(end_order)
                #            print('end_order_1a:', end_order)
                        else:
                            end_order = longest_seq[0] -1
                            end_orders.append(end_order)
                #            print('end_order_1b:', end_order)           
                                
                    else:
                        start_order = longest_seq[-1] + 1 #start at the end of the largest gap + 1
                #        print('start_order_2:', start_order)
                        start_orders.append(start_order)  
                        if longest_seq[0] -1 < first_bay: # in case one needs to end at the ending
                            end_order = last_bay
                            end_orders.append(end_order)
                #            print('end_order_2a:', end_order)
                        else:
                            end_order = longest_seq[0] -1
                            end_orders.append(end_order)
                #            print('end_order_2b:', end_order)
                            
                min_span = pd.DataFrame()
                min_span['order_no'] = order_no
                min_span['start'] = start_orders
                min_span['end'] = end_orders
                #print('min_span dataframe:', min_span)
                
                min_span[101] = np.where(((min_span['start'] <= 101) & (101 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((101 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 101) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[102] = np.where(((min_span['start'] <= 102) & (102 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((102 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 102) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[103] = np.where(((min_span['start'] <= 103) & (103 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((103 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 103) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[104] = np.where(((min_span['start'] <= 104) & (104 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((104 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 104) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[105] = np.where(((min_span['start'] <= 105) & (105 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((105 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 105) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[106] = np.where(((min_span['start'] <= 106) & (106 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((106 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 106) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[107] = np.where(((min_span['start'] <= 107) & (107 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((107 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 107) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[108] = np.where(((min_span['start'] <= 108) & (108 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((108 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 108) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[109] = np.where(((min_span['start'] <= 109) & (109 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((109 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 109) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[110] = np.where(((min_span['start'] <= 110) & (110 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((110 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 110) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[111] = np.where(((min_span['start'] <= 111) & (111 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((111 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 111) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[112] = np.where(((min_span['start'] <= 112) & (112 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((112 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 112) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[113] = np.where(((min_span['start'] <= 113) & (113 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((113 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 113) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[114] = np.where(((min_span['start'] <= 114) & (114 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((114 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 114) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[115] = np.where(((min_span['start'] <= 115) & (115 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((115 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 115) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[116] = np.where(((min_span['start'] <= 116) & (116 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((116 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 116) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[117] = np.where(((min_span['start'] <= 117) & (117 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((117 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 117) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[118] = np.where(((min_span['start'] <= 118) & (118 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((118 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 118) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[119] = np.where(((min_span['start'] <= 119) & (119 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((119 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 119) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[120] = np.where(((min_span['start'] <= 120) & (120 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((120 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 120) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[121] = np.where(((min_span['start'] <= 121) & (121 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((121 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 121) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[122] = np.where(((min_span['start'] <= 122) & (122 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((122 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 122) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[123] = np.where(((min_span['start'] <= 123) & (123 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((123 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 123) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[124] = np.where(((min_span['start'] <= 124) & (124 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((124 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 124) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[125] = np.where(((min_span['start'] <= 125) & (125 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((125 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 125) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[126] = np.where(((min_span['start'] <= 126) & (126 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((126 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 126) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[127] = np.where(((min_span['start'] <= 127) & (127 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((127 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 127) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[128] = np.where(((min_span['start'] <= 128) & (128 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((128 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 128) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[129] = np.where(((min_span['start'] <= 129) & (129 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((129 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 129) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[130] = np.where(((min_span['start'] <= 130) & (130 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((130 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 130) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[131] = np.where(((min_span['start'] <= 131) & (131 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((131 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 131) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[132] = np.where(((min_span['start'] <= 132) & (132 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((132 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 132) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[133] = np.where(((min_span['start'] <= 133) & (133 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((133 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 133) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[134] = np.where(((min_span['start'] <= 134) & (134 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((134 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 134) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[135] = np.where(((min_span['start'] <= 135) & (135 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((135 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 135) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[136] = np.where(((min_span['start'] <= 136) & (136 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((136 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 136) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[137] = np.where(((min_span['start'] <= 137) & (137 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((137 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 137) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[138] = np.where(((min_span['start'] <= 138) & (138 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((138 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 138) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[139] = np.where(((min_span['start'] <= 139) & (139 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((139 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 139) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[140] = np.where(((min_span['start'] <= 140) & (140 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((140 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 140) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[141] = np.where(((min_span['start'] <= 141) & (141 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((141 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 141) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[142] = np.where(((min_span['start'] <= 142) & (142 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((142 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 142) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[143] = np.where(((min_span['start'] <= 143) & (143 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((143 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 143) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[144] = np.where(((min_span['start'] <= 144) & (144 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((144 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 144) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[145] = np.where(((min_span['start'] <= 145) & (145 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((145 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 145) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[146] = np.where(((min_span['start'] <= 146) & (146 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((146 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 146) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[147] = np.where(((min_span['start'] <= 147) & (147 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((147 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 147) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[148] = np.where(((min_span['start'] <= 148) & (148 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((148 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 148) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[149] = np.where(((min_span['start'] <= 149) & (149 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((149 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 149) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[150] = np.where(((min_span['start'] <= 150) & (150 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((150 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 150) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[151] = np.where(((min_span['start'] <= 151) & (151 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((151 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 151) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[152] = np.where(((min_span['start'] <= 152) & (152 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((152 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 152) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[153] = np.where(((min_span['start'] <= 153) & (153 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((153 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 153) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[154] = np.where(((min_span['start'] <= 154) & (154 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((154 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 154) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[155] = np.where(((min_span['start'] <= 155) & (155 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((155 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 155) & (min_span['start'] > min_span['end'])), int(1), int(0))
                min_span[156] = np.where(((min_span['start'] <= 156) & (156 <= min_span['end']) & (min_span['start'] < min_span['end'])) | ((156 <= min_span['end']) & (min_span['start'] > min_span['end'])) | ((min_span['start'] <= 156) & (min_span['start'] > min_span['end'])), int(1), int(0))

                min_span.drop(['start','end'], axis=1, inplace=True)
                min_span.set_index('order_no', inplace=True)
                min_span.columns.name = 'LOCATION_CODE'
                #print(min_span)
                loc = min_span.copy()
                print(loc.head())
                
                loc.loc[:,'SPANS'] = loc.sum(axis=1)
                loc.reset_index(inplace = True)
#                print(loc.head())
                span_sort = loc.sort_values('SPANS')
                print(span_sort.head())
                o_spans = span_sort['order_no'].tolist()
                o_spans = [float(i) for i in o_spans]
                print('o_spans', o_spans)
                               
                search_logics = [o_spans]
                search_logic_names = ['orders_span_spans']
                
                for s, t in zip(search_logics, search_logic_names):
                    
                    search_logics = s
                    search_logic_names = t                                             
                                        
                    #empty lists for batching in algorithm
                    order_1 = []
                    order_2 = []  
                    
                    #batch_list for algorithm                    
                    
                    m = df.values #get a numpy array from df for algorithm
#                    print('m', m)
                              
                    ##timing
                    start_time = time.time()
                    
                    ###greedy algorithm   
                    for n in s:
                        for row in m:
                                if row[0] == n and np.isin(n, order_2, invert=True): #choose row according to search-logic AND only choose n's that have not been used in order_2 yet
    #                                print('n after checks:', n)          
                                    order_1.append(n)
                    #                print('row:', row)     
                                    choose = min(row)
#                                    print('choice:', choose)
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
                   
                    #---------------------------------------------
                    
                    picking_lines.append(f)
                    measurements.append(g)
                    search_logic_name.append(t)
                    order1.append(order_1)
                    order2.append(order_2)
                    timing.append(elapsed_time_secs)              
                                 
                    #---------------------------------------------
                                  
#                    print('order_1: ', order_1)
#                    print('order_2: ', order_2)
#                    print('order_1s length:' , len(order_1))
#                    print('order_2s length:' , len(order_2)) 
                    
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
    #                print('length of batch_list', len(batch_list))
                      
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
                        
                    batched.to_csv(f+'_batched_2'+g+'_'+t+'.csv',  index=None, header=False)
                
data = [picking_lines, measurements, configurations, search_logic_name, order1, order2, timing]
#print(data)

#print to csv
file = open('20190328_greedy_span_span_assignment_all_files_56_SKUs.csv', 'w')
with file:
    writer = csv.writer(file)
    writer.writerows(data)
                    
print('\007')
print('doneee!')