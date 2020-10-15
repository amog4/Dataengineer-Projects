# Create tables

def create_tables(database):
    
   
    create_database = f"""
                        create database if not exists {database}
                        
                      """

    
    drop_database =f"""
                        drop database if exists {database}
                        
                      """



    date_dim = """

    create table if not exists date_dim ( d_date_sk  integer not null primary key,
    d_date_id char(16) not null,
    d_date date not null,
    d_month_seq  integer,
    d_week_seq integer,
    d_quarter_seq integer,
    d_year integer,
    d_dow integer,
    d_moy integer,
    d_dom integer,
    d_qoy integer,
    d_fy_year integer,
    d_fy_quarter_seq integer,
    d_fy_week_seq integer,
    d_day_name char(9),
    d_quarter_name char(6),
    d_holiday char(1),
    d_weekend char(1),
    d_following_holiday char(1),
    d_first_dom integer,
    d_last_dom integer,
    d_same_day_ly integer,
    d_same_day_lq integer,
    d_current_day char(1),
    d_current_week char(1),
    d_current_month char(1),
    d_current_quarter char(1),
    d_current_year char(1)
    ) 

    """

    item = """

    create table if not exists item (i_item_sk integer not null primary key,
    i_item_id  varchar(200) not null,
    i_rec_start_date  varchar(200) null ,
    i_rec_end_date varchar(200) null,
    i_item_desc varchar(200) null,
    i_current_price varchar(200) null,
    i_wholesale_cost varchar(200) null,
    i_brand_id varchar(200)  null,
    i_brand char(50) null,
    i_class_id varchar(200)  null,
    i_class  varchar(200) null,
    i_category_id varchar(200)  null,
    i_category char(50) null,
    i_manufact_id varchar(200)  null,
    i_manufact varchar(200) null,
    i_size varchar(200) null,
    i_formulation char(20) null,
    i_color varchar(200) null,
    i_units varchar(200) null,
    i_container varchar(200)null,
    i_manager_id varchar(200)  null,
    i_product_name varchar(200) null
    ) """


    warehouse = """
    create table if not exists warehouse (w_warehouse_sk integer  not null primary key,
    w_warehouse_id  char(16),
    w_warehouse_name varchar(20),
    w_warehouse_sq_ft integer,
    w_street_number char(10),
    w_street_name varchar(60),
    w_street_type char(15),
    w_suite_number char(10),
    w_city varchar(60),
    w_county varchar(30),
    w_state char(2),
    w_zip char(10),
    w_country varchar(20),
    w_gmt_offset decimal(5,2)

    ) """

    inventory_fact = """

    create table if not exists 
    inventory_fact (inv_date_sk  integer  not null    REFERENCES date_dim(d_date_sk),

    inv_item_sk integer  not null   REFERENCES iteam(i_item_sk),

    inv_warehouse_sk integer  not null  REFERENCES warehouse(w_warehouse_sk),

    inv_quantity_on_hand  integer 


    ) """

    return  create_database,drop_database,date_dim ,item,warehouse,inventory_fact


date_dim_drop = """  drop table if  exists  date_dim_drop  """
item_drop =   """  drop table if  exists  item_drop   """
warehouse_drop =  """  drop table if exists  warehouse_drop  """
inventory_fact_drop =  """  drop table if exists  inventory_fact_drop """


create_database,drop_database ,date_dim ,item,warehouse,inventory_fact = create_tables(database = 'tpc_dc' )

drop_tables = [date_dim_drop,item_drop,warehouse_drop,inventory_fact_drop]
tables_to_create = [date_dim,item,warehouse,inventory_fact]








