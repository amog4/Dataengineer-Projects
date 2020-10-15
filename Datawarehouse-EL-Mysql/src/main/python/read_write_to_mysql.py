import mysql.connector
import math

def mysql_connection(drop_database,create_database,database,**mysql_conf):
    try:
        cnx = mysql.connector.connect(**mysql_conf)
        print("connection successful")
        
        cnx.autocommit = True

        cur = cnx.cursor()

        if drop_database != None : 
            cur = cnx.cursor()
            cur.execute(drop_database)

            print("""
                    database droped 
                """)
            cur.execute(create_database)

            print("""
                    database created
                """)

            cur.close()

        conn = mysql.connector.connect(database = database, **mysql_conf)
        
        cur = conn.cursor()

        return cur , conn

    except Exception as e:
        print(e)


def drop_func(cur,conn,drop_table_list):

    for drop_t in drop_table_list:
        cur.execute(drop_t)
        conn.commit()
    
    print('droped tables')


def create_func(cur,conn,create_table_list):

    for create_t in create_table_list:
        cur.execute(create_t)
    
    print('tables created')


def insert_data(columns,table):

    col1 = ', '.join([col for col in columns])
    col2 = ', '.join(['%s' for col in columns])
    insert_query = f"insert into {table}  ({col1}) values ({col2})"

    return insert_query




def insert_rows(cur, conn ,columns,table,data):
    
    insert_query = insert_data(columns=columns,table=table)
    

    counter = math.ceil(data.shape[0]/10000)

    for indx in range(1,counter + 1):
        start = (indx - 1) * 10000
        end = indx * 10000
        try:
            rows = data.iloc[start:end].apply(tuple,axis=1).to_list()
             

            cur.executemany(insert_query,rows)
            
            print(f'start_index {start} end_index {end}')
        except Exception as e:
            print(e)
   