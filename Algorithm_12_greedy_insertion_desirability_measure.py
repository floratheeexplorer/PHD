# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 16:37:59 2019

@author: 20304269
"""

#source: 20191107_assignment_2_lines.py

import pandas as pd

##function that counts locations
def loca(df):
    loca = len(df.loc[df['DBN_NO'] == selected, 'LOCATION_CODE'].drop_duplicates().tolist())
    return loca

#TODO: Input filenames
filenames = ['4840+4850+4854']

for f in filenames:

    df = pd.read_csv(f+'.csv')
    
    ##available locations per picking line - start with floor     
        
    if f == '4840+4850+4854':
        loca_pl_1 = 68
        loca_pl_2 = 107 
        loca_pl_3 = 72        
          
    elif f == '4887+4856+4884':
        loca_pl_1 = 94
        loca_pl_2 = 142
        loca_pl_3 = 144
        
    elif f == '4903+4902+4888':
        loca_pl_1 = 67
        loca_pl_2 = 80
        loca_pl_3 = 112
        
    elif f == '4771+4774+4773':
        loca_pl_1 = 68
        loca_pl_2 = 122
        loca_pl_3 = 142
        
    elif f == '4783+4784+4786':
        loca_pl_1 = 74
        loca_pl_2 = 146
        loca_pl_3 = 146
        
    elif f == '4793+4798+4804':
        loca_pl_1 = 68
        loca_pl_2 = 87
        loca_pl_3 = 104
        
    elif f == '4824+4812+4825':
        loca_pl_1 = 74
        loca_pl_2 = 88
        loca_pl_3 = 76
        
    else: #'4987+4986+4996'
        loca_pl_1 = 141
        loca_pl_2 = 142
        loca_pl_3 = 146
        
    ##number of branches per picking line
    count_pl_1 = 0
    count_pl_2 = 0
    count_pl_3 = 0
    
    ##empty dataframe per picking line
    df_pl_1 = pd.DataFrame()
    df_pl_2 = pd.DataFrame()
    df_pl_3 = pd.DataFrame()
    
    #list of DBNs
    DBN_list = df.DBN_NO.unique().tolist()
    DBN_list.sort()   
        
    #initial
    selected = DBN_list[0]
    #selected = rd.choice(DBN_list)
    
    ##assignment algorithm
    while not len(DBN_list) == 0:    
       
        #select the DBN with the highest max SKU
#        print('selected', selected)   
             
        #check whether line 1 still has space
        if loca_pl_1 - loca(df) >= 0:
#            print('add to 1')
            
            df_pl_1 = df_pl_1.append(df.loc[df['DBN_NO'] == selected]) #no inplace with append           
            loca_pl_1 = loca_pl_1 - loca(df_pl_1) 
            
            df = df[df['DBN_NO'] != selected]  #drop from original dataframe  
            pivot_df = pd.pivot_table(df, index =['DBN_NO'], values = ['BRANCH_NO'], aggfunc=lambda x:set((x)))
            DBN_list.remove(selected)
            if len(DBN_list) == 0:
                break
            
            stores = set(df_pl_1.loc[df_pl_1['DBN_NO'] == selected, 'BRANCH_NO'].drop_duplicates().tolist())       
            intersection = []        
    
            for index, row in pivot_df.iterrows():
                inter_calc = len(stores.intersection(row[0]))
                intersection.append(inter_calc)              
            
            selected = DBN_list[intersection.index(max(intersection))]
           
        elif loca_pl_2 - loca(df) >= 0:
            
            df_pl_2 = df_pl_2.append(df.loc[df['DBN_NO'] == selected]) #no inplace with append           
            loca_pl_2 = loca_pl_2 - loca(df_pl_2) 
            
            df = df[df['DBN_NO'] != selected]  #drop from original dataframe  
            pivot_df = pd.pivot_table(df, index =['DBN_NO'], values = ['BRANCH_NO'], aggfunc=lambda x:set((x)))
            DBN_list.remove(selected) 
            if len(DBN_list) == 0:
                break
            
            stores = set(df_pl_2.loc[df_pl_2['DBN_NO'] == selected, 'BRANCH_NO'].drop_duplicates().tolist())
            intersection = []        
    
            for index, row in pivot_df.iterrows():
                inter_calc = len(stores.intersection(row[0]))
                intersection.append(inter_calc)       
            
            selected = DBN_list[intersection.index(max(intersection))]
            
        elif loca_pl_3 - loca(df) >= 0:
            
            df_pl_3 = df_pl_3.append(df.loc[df['DBN_NO'] == selected]) #no inplace with append           
            loca_pl_3 = loca_pl_3 - loca(df_pl_3) 
            
            df = df[df['DBN_NO'] != selected]  #drop from original dataframe  
            pivot_df = pd.pivot_table(df, index =['DBN_NO'], values = ['BRANCH_NO'], aggfunc=lambda x:set((x)))
            DBN_list.remove(selected) 
            if len(DBN_list) == 0:
                break
            
            stores = set(df_pl_3.loc[df_pl_3['DBN_NO'] == selected, 'BRANCH_NO'].drop_duplicates().tolist())
            intersection = []        
    
            for index, row in pivot_df.iterrows():
                inter_calc = len(stores.intersection(row[0]))
                intersection.append(inter_calc)       
            
            selected = DBN_list[intersection.index(max(intersection))]
            
        else:
            print(f)
            print('unassigned DBNs')        
     
    df_pl_1.to_csv(f+'_loca_hist_Scen_2_df1_1_4.csv', index = False)
    df_pl_2.to_csv(f+'_loca_hist_Scen_2_df2_1_4.csv', index = False)     
    df_pl_3.to_csv(f+'_loca_hist_Scen_2_df3_1_4.csv', index = False)     
    
print('Metta')