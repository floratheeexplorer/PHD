# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 10:09:50 2020

@author: 20304269
"""
#source:20200116_data_mining_small_example

##http://rasbt.github.io/mlxtend/user_guide/frequent_patterns/apriori/
##https://www.geeksforgeeks.org/implementing-apriori-algorithm-in-python/

import numpy as np
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules 
import time
from datetime import timedelta

##function that counts locations
def loca(df):
    loca = len(df.loc[df['DBN_NO'].isin(selected), 'LOCATION_CODE'].drop_duplicates().tolist())
    return loca

#loading the datasetset
#u+z
filenames = ['4771+4773']

for f in filenames:   
                                        
    ##INPUT
    df = pd.read_csv(f+'.csv', usecols=[3,5])    
    df2 = pd.read_csv(f+'.csv')

    ##available locations per picking line - start with floor
    if f == '4771+4773':
        loca_pl_1 = 68
        loca_pl_2 = 142
        
    elif f == '4783+4784':
        loca_pl_1 = 74
        loca_pl_2 = 146
        
    elif f == '4793+4798':
        loca_pl_1 = 68
        loca_pl_2 = 87
        
    elif f == '4812+4825':
        loca_pl_1 = 88
        loca_pl_2 = 76
        
    elif f == '4837+4834':
        loca_pl_1 = 82
        loca_pl_2 = 46
        
    elif f == '4850+4854':
        loca_pl_1 = 107
        loca_pl_2 = 72
        
    elif f == '4881+4856':
        loca_pl_1 = 73
        loca_pl_2 = 142
        
    elif f == '4903+4902':
        loca_pl_1 = 67
        loca_pl_2 = 80
        
    elif f == '4955+4956':
        loca_pl_1 = 74
        loca_pl_2 = 68
        
    elif f == '4987+4986':
        loca_pl_1 = 141
        loca_pl_2 = 142
        
    elif f == '5010+5008':
        loca_pl_1 = 76
        loca_pl_2 = 97
    
    elif f == '5011+5012':
        loca_pl_1 = 102
        loca_pl_2 = 88
        
    elif f == '4759+4758':
        loca_pl_1 = 109
        loca_pl_2 = 116
        
    elif f == '4887+4884':
        loca_pl_1 = 94
        loca_pl_2 = 144
    
    else:        
        loca_pl_1 = 102
        loca_pl_2 = 116      
        
    locations = loca_pl_1 + loca_pl_2

    ##empty dataframe per picking line ## 2 for Scenario 2
    df_pl_1 = pd.DataFrame()
    picking_line_1 = []
    picking_line_1_loc = 0
    
    df_pl_2 = pd.DataFrame()
    picking_line_2 = []
    picking_line_2_loc = 0
    
    ##defining association rules ##

    DBN_df = df.pivot_table(index='BRANCH_NO', columns='DBN_NO', aggfunc=lambda x: 1, fill_value=np.NaN) #get locations per order
    for k in DBN_df.columns:
        DBN_df[k].replace(1,k,inplace = True)
            
    #converting the dataframe into a list of lists
    records = DBN_df.T.apply(lambda x: x.dropna().tolist()).tolist()    
    #print(records)
    
    #generating frequent itemsets
    te = TransactionEncoder()
    te_ary = te.fit(records).transform(records)
    data = pd.DataFrame(te_ary, columns=te.columns_)
    
    #timing_input#################################################
    start_time = time.time()
    
    ##frq_items with apriori algorithm
    frq_items = apriori(data, min_support = 0.1, use_colnames = True) 
    frq_items = frq_items.sort_values(['support'], ascending =[False]) 
#    frq_items.to_csv('frq_items.csv')
    
    #association rule formula
    rules = association_rules(frq_items, metric ="lift", min_threshold = 1.1)  
    rules = rules.sort_values(['support','confidence', 'lift'], ascending =[False, False, False]) 
    
    antecedents = rules['antecedents'].tolist()
    rules['antecedents'] = [list(x) for x in antecedents]
    consequents = rules['consequents'].tolist()
    rules['consequents'] = [list(x) for x in consequents]
    rules['association'] = rules['antecedents'] + rules['consequents']
    rules['association_sets'] = rules['association'].apply(lambda x: set(x))
    
    rules['new'] = rules.association_sets.apply(tuple)
    rules = rules.sort_values(['support','confidence', 'lift'], ascending =[False, False, False]).drop_duplicates('new')
    rules.reset_index(drop=True, inplace=True)
    
    ##allocation algorithm
    while not rules.empty:                    
        if loca_pl_1 - picking_line_1_loc >= 0:

            if rules['association'][0] not in picking_line_1:
                selected = list(set(rules['association'][0]) - set(picking_line_1))
    
                picking_line_1_loc = picking_line_1_loc + loca(df2)
    
                picking_line_1.extend(selected)
                rules = rules.drop(rules.index[0])
                rules.reset_index(drop=True, inplace=True)            
                df_pl_1 = df_pl_1.append(df2.loc[df2['DBN_NO'].isin(selected)])
                locations = locations - picking_line_1_loc
                
        elif loca_pl_2 - picking_line_2_loc >= 0:

            if rules['association'][0] not in picking_line_1 and rules['association'][0] not in picking_line_2:
                selected = list(set(rules['association'][0]) - set(picking_line_1) - set(picking_line_2))
    
                picking_line_2_loc = picking_line_2_loc + loca(df2)
    
                picking_line_2.extend(selected)
                rules = rules.drop(rules.index[0])
                rules.reset_index(drop=True, inplace=True)            
                df_pl_2 = df_pl_2.append(df2.loc[df2['DBN_NO'].isin(selected)])
                locations = locations - picking_line_2_loc
                  
        else:
            if locations == picking_line_1_loc + picking_line_2_loc:
                print('no more rules needed')   
                break
      
    #some open locations     
    if locations - (picking_line_1_loc + picking_line_2_loc) != 0:
        print(f, 'more rules needed')
        lines = list(set(picking_line_1)) + list(set(picking_line_2))
        DBNs_list = df2['DBN_NO'].unique().tolist()
        picking_line_leftovers = list(set(DBNs_list) - set(lines))
        
        picking_line_2.extend(picking_line_leftovers)
        df_pl_2 = df_pl_2.append(df2.loc[df2['DBN_NO'].isin(picking_line_leftovers)])
                
    picking_line_1 = list(set(picking_line_1))
#    print('picking_line_1 outside', picking_line_1)
    
    picking_line_2 = list(set(picking_line_2))
#    print('picking_line_2 outside', picking_line_2)
    
    #timing_output #########################################
    elapsed_time_secs = time.time() - start_time
    msg = 'Runtime in sec %s' % timedelta(seconds=round(elapsed_time_secs))
    print(f, msg)

    ##OUTPUT
    df_pl_1.to_csv(f+'_loca_hist_Scen_1_df1_1_5.csv', index = False)
    df_pl_2.to_csv(f+'_loca_hist_Scen_1_df2_1_5.csv', index = False)
