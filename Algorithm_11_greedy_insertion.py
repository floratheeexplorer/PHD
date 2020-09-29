

#20191107_assignment_2_lines.py

import pandas as pd

##function that prints max SKUs
def sort_max_SKU(df):
    groups = df.groupby(['DBN_NO','SKU_NO']).BRANCH_NO.nunique()
    sorted_groups = groups.reset_index('SKU_NO').sort_values(by=['BRANCH_NO'], ascending=False)
    return sorted_groups

##function that counts number of branches
def count_branch(df):
    if not df.empty:
        branch_sum = df.BRANCH_NO.nunique()
    else:
        branch_sum = 0
    return branch_sum

##function that counts locations
def loca(df):
    loca = len(df.loc[df['DBN_NO'] == selected, 'LOCATION_CODE'].drop_duplicates().tolist())
    return loca

filenames = ['4840+4850+4854', '4887+4856+4884', '4903+4902+4888', '4771+4774+4773', '4783+4784+4786', '4793+4798+4804', '4824+4812+4825', '4987+4986+4996']

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
  
    ###check the number of SKUs that need to go on floor or into rack
    #storage = pd.DataFrame()
    #
    #SKUs = df.groupby(['SKU_NO']).SKU_NO.unique().tolist()
    #print('len(SKUs)', len(SKUs))
    #storage['SKU_NO'] = [x[0] for x in SKUs] #remove square brackets
    #stock = df.groupby(['SKU_NO'])['QTY*CBM'].sum().tolist()
    ##print('len(stock)', len(stock))
    #storage['STOCK'] = stock
    #carton_num = []
    #for s in stock:
    #    cartons = np.round(s/0.04)
    #    carton_num.append(cartons)   
    #storage['NO_CARTONS'] = [int(x) for x in carton_num] #change to int instead of float
    #pick_freq = df.groupby(['SKU_NO']).BRANCH_NO.nunique().tolist()
    ##print(pick_freq)
    #storage['PICK_FREQ'] = pick_freq
    ##                print(storage)   
    #
    #SKUs_rack_num = len(storage[storage['NO_CARTONS'] <= 6]['SKU_NO'].tolist())
    #SKUs_floor_num = len(storage[storage['NO_CARTONS'] > 6]['SKU_NO'].tolist())
    #
    #print('SKUs_rack_num', SKUs_rack_num)
    #print('SKUs_floor_num', SKUs_floor_num)
    
    ##assignment algorithm
    while not df.empty:    
       
        #select the DBN with the highest max SKU
        selected = sort_max_SKU(df).index[0] #don't change index before to get DBN selected
#        print('selected', selected)
#     
#        print('loca_pl_1', loca_pl_1)
#        print('loca_pl_2', loca_pl_2)  
        
        #check whether adding SKU to line 1 increases number of branches more for line 1 or line 2
        if loca_pl_1 - loca(df) >= 0 and (count_branch(df_pl_1.append(df.loc[df['DBN_NO'] == selected])) - count_pl_1) <= (count_branch(df_pl_2.append(df.loc[df['DBN_NO'] == selected])) - count_pl_2):
#            print('compare 1', count_branch(df_pl_1.append(df.loc[df['DBN_NO'] == selected])) - count_pl_1)
#            print('compare 2', count_branch(df_pl_2.append(df.loc[df['DBN_NO'] == selected])) - count_pl_2)
#            print('add to 1')
            df_pl_1 = df_pl_1.append(df.loc[df['DBN_NO'] == selected]) #no inplace with append           
            loca_pl_1 = loca_pl_1 - loca(df_pl_1)  
            count_pl_1 = count_branch(df_pl_1)   
#            print('count_pl_1', count_pl_1)          
       
        elif loca_pl_2 - loca(df) >= 0:
#            print('add to 2')
            df_pl_2 = df_pl_2.append(df.loc[df['DBN_NO'] == selected]) #no inplace with append
            loca_pl_2 = loca_pl_2 - loca(df_pl_2)
            count_pl_2 = count_branch(df_pl_2) 
#            print('count_pl_2', count_pl_2)       
            
        elif loca_pl_3 - loca(df) >= 0:
#            print('add to 2')
            df_pl_3 = df_pl_3.append(df.loc[df['DBN_NO'] == selected]) #no inplace with append
            loca_pl_3 = loca_pl_3 - loca(df_pl_3)
            count_pl_3 = count_branch(df_pl_3) 
#            print('count_pl_2', count_pl_2)  
        
        else:
            print('f', f)
            print('unassigned DBNs')
        
        df = df[df['DBN_NO'] != selected]  #drop from original dataframe    
    
    df_pl_1.to_csv(f+'_loca_hist_Scen_2_df1_1_3.csv', index = False)
    df_pl_2.to_csv(f+'_loca_hist_Scen_2_df2_1_3.csv', index = False)   
    df_pl_3.to_csv(f+'_loca_hist_Scen_2_df3_1_3.csv', index = False)

print('Metta')     