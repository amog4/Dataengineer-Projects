
import sys, os
import yaml
import glob
s =  '{}/../../../../retailer/data/dat/*'.format(os.path.dirname(os.path.abspath(__file__)) ) 

files = ['item.dat','date_dim.dat','inventory.dat','warehouse.dat']


for _ in files:
    print( glob.glob(s))