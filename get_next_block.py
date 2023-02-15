import create_tables
import sqlite3                                                                                                      
from pathlib import Path

from datetime import datetime
from time import sleep

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
def update_end_date_for_a_block(conn, id):
    cur = conn.cursor()
    cur.execute(update_end_date_sql,(id,))
    cur.commit()

def get_next_block(conn)->bool:
    if conn is None:
        return None
    conn.isolation_level = None
    with conn:
        c=conn.cursor()
        
        c.execute("begin")
        c.execute(get_next_block_to_work_on)
        #first_row =c.fetchone()
        first_row = next(c, [None])[0] # https://stackoverflow.com/questions/7011291/how-to-get-a-single-result-from-a-sql-query-in-python
        #if first_row is None:
        #    c.execute(get_next_block_to_work_on_first_sql)
        #    #c.fetchone()
        #    first_row = next(c, [None])[0]
        print(first_row)
        if first_row ==None:
            return False;

        c.execute(update_begin_date_sql,(datetime.now(),first_row))
        c.execute("commit")
        return first_row
        #except Exception as e:
        #    print(e)
        #    c.execute("rollback")
    return False

def get_simulations():
    
    retries=100
    next_block=[]
    for i in range(1,retries):
        try:
            conn = create_tables.create_connection(create_tables.db_file)
            next_block=get_next_block(conn)
            break
        except Exception as e:
            print(e)
            sleep(1)
            
    if next_block is False:
        return False
    
    new_conn = create_tables.create_connection(create_tables.db_file)
    rows= get_simulations_for_a_block(new_conn,(next_block,))    
    return rows
    
if __name__=="__main__":
    get_simulations()    
