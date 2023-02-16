import create_tables
import sqlite3                                                                                                      
from pathlib import Path

from datetime import datetime
from time import sleep
from random import uniform

get_simulations_for_a_block_sql = """select * from simulations where fk_blocks_id=?;"""

get_next_block_to_work_on = """select min(id) from blocks where begin_date is null;"""
#This is for the 
#get_next_block_to_work_on_first_sql = """select min(id) from blocks;"""
update_begin_date_sql = """update blocks set begin_date=? where id=?;"""
update_end_date_sql = """update blocks set end_date=? where id=?;"""

def get_simulations_for_a_block(conn,data):
    cur = conn.cursor()
    cur.execute(get_simulations_for_a_block_sql, data)
    rows = cur.fetchall()
    
    return rows

def update_end_date_for_a_block(id):
    retries=0
    while retries<5:
        create_tables.create_connection()
        if conn is None:
            retries+=1
            sleep_time = uniform(0.1,4)
            print(f"Sleeping for {sleep_time}")
            sleep(sleep_time)
            continue
        try:
            with conn:
                cur = conn.cursor()
                current_date = datetime.now()
                print(f'Update end_date_for_a_block;Current date is: {current_date}') 
                cur.execute(update_end_date_sql,(current_date,id))
                return True
        except Exception as e :
            print(e)
            sleep_time = uniform(0.1,4)
            print(f"Sleeping for {sleep_time}")
            sleep(sleep_time)
            retries+=1
            
    raise Exception("update_end_date_for_a_block threw an exception after {retries}")


def get_next_block(conn)->bool:
    if conn is None:
        return None
    first_row = None
    c=conn.cursor()
    retries=0
    while retries<5:
        try:
            with conn:
                c.execute("begin")
                c.execute(get_next_block_to_work_on)
 
                first_row = next(c, [None])[0] # https://stackoverflow.com/questions/7011291/how-to-get-a-single-result-from-a-sql-query-in-python
                print(f'first_row before update {first_row}')
                if first_row is None:
                    print(f'first_row is {first_row}')
                    return False;
                current_time = datetime.now()
                print(f'Current time is : {current_time}')
                c.execute(update_begin_date_sql,(current_time,first_row))
                print(f'first_row after update {first_row}')
                return first_row
        except Exception as e :
            print(e)
            sleep_time = uniform(0.1,4)
            print(f"Sleeping for {sleep_time}")
            sleep(sleep_time)
            retries+=1
            
    return first_row

def get_simulations():
    
    retries=100
    next_block=[]
    #for i in range(1,retries):
    #    try:
    conn = create_tables.create_connection(create_tables.db_file,timeout=30, isolation_level=None)
    next_block=get_next_block(conn)
    #        break
    #    except Exception as e:
    #        print(e)
    #        sleep(1)
            
    if next_block is False:
        return False
    
    new_conn = create_tables.create_connection(create_tables.db_file)
    rows= get_simulations_for_a_block(new_conn,(next_block,))    
    return rows
    
if __name__=="__main__":
    get_simulations()    
