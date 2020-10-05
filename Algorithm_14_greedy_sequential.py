# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 11:57:58 2019

@author: 20304269
"""

#source:20191114_assignment_2_lines.py

import pandas as pd
import numpy as np

############ alternative ############
###only rack SKUs
#if df['LOCATION_CODE'].str.contains(r'C(?!$)').any():
#    df_rack = df[df['LOCATION_CODE'].str.contains(r'C(?!$)')]
#    print(df_rack.head())

###only floor SKUs
#df_floor = pd.concat([df,df_rack,df_rack]).drop_duplicates(keep=False)
#print(df_floor.info())

#TODO: input filenames  
filenames = ['4783+4784+4786']
      
for f in filenames:
    
    scenarios = ['Scen_2_']
    
    for c in scenarios:
    
        lines = ['df1', 'df2', 'df3']
#        lines = ['df3']
        
        for l in lines: 
            
            #TODO: choose an assigment            
#            assignment = ['_1_1', '_1_2','_1_3', '_1_4'] 
            assignment = ['_1_5'] 

            for a in assignment:
                
                #TODO: choose an extension            
                extension = [''] #,'_batched_2_stop_ratio_o_rand', '_batched_2_span_stops_orders_smallest_entry'] -> batching before SKU location
                 
                for e in extension:                   
    
                    df = pd.read_csv(f+'_loca_hist_'+c+l+a+e+'.csv')
#                    print(df.head())
                                   
                    ##all line configuration options at Kuilsriver DC
                    ##ground A floor 76 locations
                    floor_ground_A_loca = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176]
                    ##ground B floor 68 locations
                    floor_ground_B_loca = [101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168]
                    
                    ##A module floor loca 46 locations
                    floor_loca_module_A = [101,102,103,104,105,106,107,108,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,127,128,129,130,131,159,160,161,162,163,164,165,166,167,169,170,171,172,173,174,175,176]
                    ##A module rack loca 17 x 3 x 3 x 3 (51, 102, 153)
                    rack_loca_lev_1_module_A = [13901,13902,13903,14001,14002,14003,14101,14102,14103,14201,14202,14203,14301,14302,14303,14401,14402,14403,14501,14502,14503,14601,14602,14603,14701,14702,14703,14901,14902,14903,15001,15002,15003,15101,15102,15103,15201,15202,15203,15301,15302,15303,15401,15402,15403,15501,15502,15503,15601,15602,15603]
                    rack_loca_lev_2_module_A = [13901,13902,13903,13904,13905,13906,14001,14002,14003,14004,14005,14006,14101,14102,14103,14104,14105,14106,14201,14202,14203,14204,14205,14206,14301,14302,14303,14304,14305,14306,14401,14402,14403,14404,14405,14406,14501,14502,14503,14504,14505,14506,14601,14602,14603,14604,14605,14606,14701,14702,14703,14704,14705,14706,14901,14902,14903,14904,14905,14906,15001,15002,15003,15004,15005,15006,15101,15102,15103,15104,15105,15106,15201,15202,15203,15204,15205,15206,15301,15302,15303,15304,15305,15306,15401,15402,15403,15404,15405,15406,15501,15502,15503,15504,15505,15506,15601,15602,15603,15604,15605,15606]
                    rack_loca_lev_3_module_A = [13901,13902,13903,13904,13905,13906,13907,13908,13909,14001,14002,14003,14004,14005,14006,14007,14008,14009,14101,14102,14103,14104,14105,14106,14107,14108,14109,14201,14202,14203,14204,14205,14206,14207,14208,14209,14301,14302,14303,14304,14305,14306,14307,14308,14309,14401,14402,14403,14404,14405,14406,14407,14408,14409,14501,14502,14503,14504,14505,14506,14507,14508,14509,14601,14602,14603,14604,14605,14606,14607,14608,14609,14701,14702,14703,14704,14705,14706,14707,14708,14709,14901,14902,14903,14904,14905,14906,14907,14908,14909,15001,15002,15003,15004,15005,15006,15007,15008,15009,15101,15102,15103,15104,15105,15106,15107,15108,15109,15201,15202,15203,15204,15205,15206,15207,15208,15209,15301,15302,15303,15304,15305,15306,15307,15308,15309,15401,15402,15403,15404,15405,15406,15407,15408,15409,15501,15502,15503,15504,15505,15506,15507,15508,15509,15601,15602,15603,15604,15605,15606,15607,15608,15609]
                    
                    ##B module floor loca 47 locations
                    floor_loca_module_B = [101,102,103,104,105,106,107,108,110,111,112,113,114,115,116,117,118,119,120,121,122,123,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,143,144,145,146,147,148,149,150]
                    ##B module rack loca 11 x 3 x 3 x 3 (33, 66, 99)
                    rack_loca_lev_1_module_B = [15301,15302,15303,15401,15402,15403,15501,15502,15503,15601,15602,15603,15701,15702,15703,15801,15802,15803,15901,15902,15903,16001,16002,16003,16201,16202,16203,16301,16302,16303,16401,16402,16403]
                    rack_loca_lev_2_module_B = [15301,15302,15303,15304,15305,15306,15401,15402,15403,15404,15405,15406,15501,15502,15503,15504,15505,15506,15601,15602,15603,15604,15605,15606,15701,15702,15703,15704,15705,15706,15801,15802,15803,15804,15805,15806,15901,15902,15903,15904,15905,15906,16001,16002,16003,16004,16005,16006,16201,16202,16203,16204,16205,16206,16301,16302,16303,16304,16305,16306,16401,16402,16403,16404,16405,16406]
                    rack_loca_lev_3_module_B = [15301,15302,15303,15304,15305,15306,15307,15308,15309,15401,15402,15403,15404,15405,15406,15407,15408,15409,15501,15502,15503,15504,15505,15506,15507,15508,15509,15601,15602,15603,15604,15605,15606,15607,15608,15609,15701,15702,15703,15704,15705,15706,15707,15708,15709,15801,15802,15803,15804,15805,15806,15807,15808,15809,15901,15902,15903,15904,15905,15906,15907,15908,15909,16001,16002,16003,16004,16005,16006,16007,16008,16009,16201,16202,16203,16204,16205,16206,16207,16208,16209,16301,16302,16303,16304,16305,16306,16307,16308,16309,16401,16402,16403,16404,16405,16406,16407,16408,16409]
                    
                    ##check the number of SKUs that need to go on floor or into racks
                    #storage dataframe input
                    storage = pd.DataFrame()
                    
                    SKUs = df.groupby(['SKU_NO']).SKU_NO.unique().tolist()
    #                print('len(SKUs)', len(SKUs))
                    storage['SKU_NO'] = [x[0] for x in SKUs] #remove square brackets
                    stock = df.groupby(['SKU_NO'])['QTY*CBM'].sum().tolist()
                    #print('len(stock)', len(stock))
                    storage['STOCK'] = stock
                    carton_num = []
                    for s in stock:
                        cartons = np.round(s/0.04)
                        carton_num.append(cartons)   
                    storage['NO_CARTONS'] = [int(x) for x in carton_num] #change to int instead of float
                    pick_freq = df.groupby(['SKU_NO']).BRANCH_NO.nunique().tolist()
                    #print(pick_freq)
                    storage['PICK_FREQ'] = pick_freq
#                    print(storage)   
                    
                    SKUs_rack_num = len(storage[storage['NO_CARTONS'] <= 6]['SKU_NO'].tolist())
                    SKUs_floor_num = len(storage[storage['NO_CARTONS'] > 6]['SKU_NO'].tolist())
                    
#                    print('SKUs_rack_num', SKUs_rack_num)
#                    print('SKUs_floor_num', SKUs_floor_num) 
                    
                    SKU_total = SKUs_floor_num+SKUs_rack_num
#                    print('SKU_total', SKU_total)
                                                   
                    #config B: A ground floor and B module
                    if f == '4783+4784+4786' or f == '4824+4812+4825':
                        if SKUs_floor_num > len(floor_loca_module_B) and SKU_total <= len(floor_ground_A_loca):
                            floor_loca = floor_ground_A_loca
                            rack_loca = []
    
                        else: 
                            floor_loca = floor_loca_module_B
                            if SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_1_module_B):
                                rack_loca = rack_loca_lev_1_module_B
                            elif SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_2_module_B):
                                rack_loca = rack_loca_lev_2_module_B
                            else:
                                rack_loca = rack_loca_lev_3_module_B
                                
                    #config C: B ground floor and B module (47 floor loc) and A module (46 floor loc)
                    if f == '4840+4850+4854' or f == '4771+4774+4773' or f == '4793+4798+4804':
                        if SKUs_floor_num > len(floor_loca_module_B) and SKU_total <= len(floor_ground_B_loca):
                            floor_loca = floor_ground_B_loca
                            rack_loca = []
                            
                        elif SKU_total <= len(floor_loca_module_B)+len(rack_loca_lev_3_module_B):                   
                            floor_loca = floor_loca_module_B
                            if SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_1_module_B):
                                rack_loca = rack_loca_lev_1_module_B
                            elif SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_2_module_B):
                                rack_loca = rack_loca_lev_2_module_B
                            else:
                                rack_loca = rack_loca_lev_3_module_B
                                
                        else:
                            floor_loca = floor_loca_module_A
                            if SKU_total-len(floor_loca_module_A) <= len(rack_loca_lev_1_module_A):
                                rack_loca = rack_loca_lev_1_module_A
                            elif SKU_total-len(floor_loca_module_A) <= len(rack_loca_lev_2_module_A):
                                rack_loca = rack_loca_lev_2_module_A
                            else:
                                rack_loca = rack_loca_lev_3_module_A   
                                
                    #config D: B ground floor and B module
                    if f == '4903+4902+4888':
                        if SKUs_floor_num > len(floor_loca_module_B) and SKU_total <= len(floor_ground_B_loca):
                            floor_loca = floor_ground_B_loca
                            rack_loca = []
    
                        else: 
                            floor_loca = floor_loca_module_B
                            if SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_1_module_B):
                                rack_loca = rack_loca_lev_1_module_B
                            elif SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_2_module_B):
                                rack_loca = rack_loca_lev_2_module_B
                            else:
                                rack_loca = rack_loca_lev_3_module_B
                                                           
                    #config G: A+B module
                    if f == '4887+4856+4884' or f == '4987+4986+4996':
                        
                        if SKU_total <= len(floor_loca_module_B)+len(rack_loca_lev_3_module_B):                   
                            floor_loca = floor_loca_module_B
                            if SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_1_module_B):
                                rack_loca = rack_loca_lev_1_module_B
                            elif SKU_total-len(floor_loca_module_B) <= len(rack_loca_lev_2_module_B):
                                rack_loca = rack_loca_lev_2_module_B
                            else:
                                rack_loca = rack_loca_lev_3_module_B
                                
                        else:
                            floor_loca = floor_loca_module_A
                            if SKU_total-len(floor_loca_module_A) <= len(rack_loca_lev_1_module_A):
                                rack_loca = rack_loca_lev_1_module_A
                            elif SKU_total-len(floor_loca_module_A) <= len(rack_loca_lev_2_module_A):
                                rack_loca = rack_loca_lev_2_module_A
                            else:
                                rack_loca = rack_loca_lev_3_module_A               
                   
    #                print(f, 'len(floor_loca): ', len(floor_loca), 'len(rack_loca): ', len(rack_loca))  
                    print(f, l, a, len(floor_loca), len(rack_loca))
                    
                    ##create dictionaries
                    floor_dict = {}
                    rack_dict = {}
                      
                    ##SKUs to floor (no rack!)
                    if len(rack_loca) == 0:
                        SKUs_floor_df = storage.sort_values(by = ['PICK_FREQ'], ascending = False)
    #                    print('SKUs_floor_df', SKUs_floor_df.head())
                        SKUs_floor = SKUs_floor_df['SKU_NO'].tolist()
#                        print('len', len(SKUs_floor))
                    
                        for g in range(len(SKUs_floor)):
    #                        print('g', g)
                            floor = floor_loca[0]
                            floor_loca.remove(floor)
                            floor_dict.update({SKUs_floor[g] : floor})
                    
    #                    print('floor_dict', floor_dict)   
                            
                        df.drop('LOCATION_CODE', axis=1, inplace=True)
                        #print(df.info())                
                        df.insert(4, 'LOCATION_CODE', df['SKU_NO'])
                        #print(df.info())    
                                       
                        df['LOCATION_CODE'].replace(floor_dict, inplace=True)
                                              
                    ##SKUs to rack
                    else:
                        SKUs_floor_df = storage.sort_values(by = ['NO_CARTONS', 'PICK_FREQ'], ascending = [False, False])
                        open_floor = len(floor_loca)

                        SKUs_floor_df = SKUs_floor_df.iloc[:open_floor]

                        SKUs_floor = SKUs_floor_df['SKU_NO'].tolist()                 
                                            
                        for g in range(len(SKUs_floor)):

                            floor = floor_loca[0]
                            floor_loca.remove(floor)
                            floor_dict.update({SKUs_floor[g] : floor})                
                        
                        SKUs_rack_df = storage.drop(SKUs_floor_df.index, axis=0)

                        SKUs_rack = SKUs_rack_df['SKU_NO'].tolist()
                        
                        for r in range(len(SKUs_rack)):
                            
                            if rack_loca == []:
                                print('break')
                                break
                            
                            rack = rack_loca[0]
                            rack_loca.remove(rack)
                            rack_dict.update({SKUs_rack[r] : rack})                          
                                                         
                        df.drop('LOCATION_CODE', axis=1, inplace=True)
             
                        df.insert(4, 'LOCATION_CODE', df['SKU_NO'])
                   
                        df.dropna(inplace=True) ##for PEP
                                       
                        df['LOCATION_CODE'].replace(floor_dict, inplace=True)
                        df['LOCATION_CODE'].replace(rack_dict, inplace=True)
                        
                        df.loc[df['LOCATION_CODE'].astype(str).str.len() == 5, 'LOCATION_CODE'] = df['LOCATION_CODE'].astype(str).str[:-2].astype(np.int64) #make one bay instead of different rack-shelves
                        
                    ##original DF
#                    df.to_csv(f+'_SKU_location_org'+c+l+a+e+'.csv', index = False) #header=None,                     
                    
                    pack_slip = int(np.round(df['QTY*CBM'].sum() / 0.04))
    #                print(pack_slip)
                    df['PACK_SLIP_CODE_NO'] = pack_slip                
                    
                    #LINE_NO	PICK_BATCH_NO	BRANCH_NO	LOCATION_CODE	SKU_NO	PICK_QTY	PACK_SLIP_CODE_NO
                    df_new = df[['LINE_NO', 'DBN_NO', 'BRANCH_NO', 'LOCATION_CODE', 'SKU_NO', 'REQUIRE_QTY', 'PACK_SLIP_CODE_NO']]
                    df_new.sort_values(['BRANCH_NO','LOCATION_CODE'], ascending=[True,True], inplace=True)                
                    df_new = df_new.drop_duplicates(subset=['BRANCH_NO', 'LOCATION_CODE', 'SKU_NO'])  
                    
#                    print(df_new.head())
                    
                    df_new.to_csv(f+'_SKU_location_freq_'+c+l+a+e+'.csv', index = False, header=None)                             
              
print('Metta')