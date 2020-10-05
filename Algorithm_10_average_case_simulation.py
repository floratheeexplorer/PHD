# -*- coding: utf-8 -*-

"""

Created on Thu Dec 12 16:08:11 2019


"""
 

#simulations to measure the difference between z and u

 
import random

 
def u_vs_z(no_bays,no_prod,gap):

    #no_bays = 60 #length of line
    #no_prod = 3 #number of SKUs
    #gap = 3 #length of gap
    #file_name = str(no_bays) + '_' + str(no_prod)

   

    prod_place = [i for i in range(no_prod)]
    rand_num = [i for i in range(no_bays)]
    list_above = []
    list_under = []

    random.shuffle(rand_num)
  

    for j in range(no_bays):
        if rand_num[j] < no_prod:
            prod_place[rand_num[j]] = j          
  
    #calculate distance

    prod_in_follow = sorted(prod_place)
    #print(prod_in_follow)  

    space = 0
    best_space = 0       

    for i in range(no_prod):

        space = (no_bays + prod_in_follow[i] - prod_in_follow[i-1]) % no_bays
        if space > best_space:
            best_space = space
   
    u_pick_length = no_bays - best_space + 1   

    for i in range (no_prod):
        if prod_in_follow[i] <= no_bays/2:
            list_above.append(prod_in_follow[i])
        else:
            list_under.insert(0, no_bays - prod_in_follow[i])   

    list_above.append(1000)
    list_under.append(1000)   
    #print(list_above)
    #print(lys_under)

    z_pick_length = 0   

    if list_above[0] <= list_under[0]:
        is_above = True
        previous_stop = list_above.pop(0)
    else:
        is_above = False
        previous_stop = list_under.pop(0)   

    #print(list_above)
    #print(list_under)
   
    for i in range(no_prod-1):
        if list_above[0] <= list_under[0]:
            if is_above == True:
                z_step_length = list_above[0] - previous_stop
                previous_stop = list_above.pop(0)
            else:
                z_step_length = list_above[0] - previous_stop + gap
                is_above = True
                previous_stop = list_above.pop(0)
        else:
            if is_above == True:
                z_step_length = list_under[0] - previous_stop + gap
                is_above = False
                previous_stop = list_under.pop(0)
            else:
                z_step_length = list_under[0] - previous_stop
                previous_stop = list_under.pop(0)
                
        z_pick_length = z_pick_length + z_step_length

        #print(z_step_length)
        #print(z_pick_length)
        #print(list_above)
        #print(list_under)

    return u_pick_length - z_pick_length
    #print(str(u_pick_length) + ' ' + str(z_pick_length))
    #print(u_pick_length - z_pick_length)

for k in range(29): #number of SKUs

    total = 0
    for j in range(1000):
        total = total + u_vs_z(60,k+2,3)
        
    print(str(k+2) + ' ' + str(total/1000))