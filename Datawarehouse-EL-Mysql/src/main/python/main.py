import os
import sys
sys.path.insert(1,'{}/../'.format(os.path.dirname(__file__)))
from utils import *
import pandas as pd 
import yaml
import glob
import numpy as np
from create_tables import drop_tables,tables_to_create,create_database,drop_database

from read_write_to_mysql import mysql_connection,drop_func,create_func,insert_data,insert_rows

with open('{}/../config/config.yml'.format(os.path.dirname(__file__)),'rb') as conf_file:
    _ = yaml.load(conf_file,Loader=yaml.FullLoader)

mysql_conf = _.get('mysql')


cur,conn = mysql_connection(drop_database=drop_database ,
                create_database =create_database ,
                database = 'tpc_dc',**mysql_conf)


tables = ['date_dim','item','warehouse','inventory_fact']

drop_func(cur =cur,conn = conn,drop_table_list = drop_tables)

create_func(cur =cur,conn = conn,create_table_list=tables_to_create)

files = ['date_dim.dat','item.dat','warehouse.dat','inventory.dat']

s =  '{}/../../../../retailer/data/dat/'.format(os.path.dirname(os.path.abspath(__file__)) ) 

for coun ,_ in enumerate(files):
    file = os.path.join('/home/amogh/Documents/data-engineering-workflows/retailer/data/dat/',_)

    print(file)

    df = pd.read_csv(file,sep="|",skipinitialspace=True)

    df = trim_all_columns(df)


    col = list(df.columns)

            

    mysql_datatypes(col=col,df = df )

    df = df.replace({np.nan: None})

    
    insert_rows(cur = cur, conn = conn ,columns = col,table =  tables[coun],data = df)
    


cur.close()
conn.close()

