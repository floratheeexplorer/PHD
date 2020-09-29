# -*- coding: utf-8 -*-

"""

Created on Thu Dec 12 16:08:11 2019

 

@author: SVISAGIE

"""

 

#Hardloop 'n klomp simulasies om die verskil tussen z en u te meet

 

import random

 

def u_vs_z(aant_bays,aant_prod,gap):

    #aant_bays = 60 #length of line

    #aant_prod = 3 #number of SKUs

    #gap = 3 #length of gap

    #file_naam = str(aant_bays) + '_' + str(aant_prod)

   

    prod_plek = [i for i in range(aant_prod)]

    rand_nums = [i for i in range(aant_bays)]

    lys_bo = []

    lys_onder = []

    random.shuffle(rand_nums)

   

    for j in range(aant_bays):

        if rand_nums[j] < aant_prod:

            prod_plek[rand_nums[j]] = j

           

    

    #bereken u afstand

    prod_in_volg = sorted(prod_plek)

    #print(prod_in_volg)

   

    gat = 0

    beste_gat = 0

       

    for i in range(aant_prod):

        gat = (aant_bays + prod_in_volg[i] - prod_in_volg[i-1]) % aant_bays

        if gat > beste_gat:

            beste_gat = gat

   

    u_pick_lengte = aant_bays - beste_gat + 1

   

    for i in range (aant_prod):

        if prod_in_volg[i] <= aant_bays/2:

            lys_bo.append(prod_in_volg[i])

        else:

            lys_onder.insert(0,aant_bays - prod_in_volg[i])

   

    lys_bo.append(1000)

    lys_onder.append(1000)

   

    #print(lys_bo)

    #print(lys_onder)

    z_pick_lengte = 0

   

    if lys_bo[0] <= lys_onder[0]:

        is_bo = True

        vorige_stop = lys_bo.pop(0)

    else:

        is_bo = False

        vorige_stop = lys_onder.pop(0)

   

    #print(lys_bo)

    #print(lys_onder)
   

    for i in range(aant_prod-1):

        if lys_bo[0] <= lys_onder[0]:

            if is_bo == True:

                z_stap_lengte = lys_bo[0] - vorige_stop

                vorige_stop = lys_bo.pop(0)

            else:

                z_stap_lengte = lys_bo[0] - vorige_stop + gap

                is_bo = True

                vorige_stop = lys_bo.pop(0)

        else:

            if is_bo == True:

                z_stap_lengte = lys_onder[0] - vorige_stop + gap

                is_bo = False

                vorige_stop = lys_onder.pop(0)

            else:

                z_stap_lengte = lys_onder[0] - vorige_stop

                vorige_stop = lys_onder.pop(0)

        z_pick_lengte = z_pick_lengte + z_stap_lengte

        #print(z_stap_lengte)

        #print(z_pick_lengte)

        #print(lys_bo)

        #print(lys_onder)

    return u_pick_lengte - z_pick_lengte

    #print(str(u_pick_lengte) + ' ' + str(z_pick_lengte))

    #print(u_pick_lengte - z_pick_lengte)

for k in range(29): #number of SKUs

    total = 0

    for j in range(1000):

        total = total + u_vs_z(60,k+2,3)

    print(str(k+2) + ' ' + str(total/1000))