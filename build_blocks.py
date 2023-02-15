import create_tables
import sqlite3
import math

from pathlib import Path

block_size=70

create_index_sql = """CREATE INDEX 
                      idx_simulations_fk_blocks_id
                      ON simulations (fk_blocks_id);"""

find_max_simulation_id_sql = """SELECT max(id) FROM simulations;"""

update_simulations_fk_block_id_sql = """update simulations set fk_blocks_id=? where id>=? and id<?;"""

create_new_block_sql = """INSERT into blocks (id,name) Values (?,?);"""

drop_index_sql ="""DROP INDEX IF EXISTS idx_simulations_fk_blocks_id ;"""

def find_highest_id(conn):
    cur = conn.cursor()
    cur.execute(find_max_simulation_id_sql)
    rows = cur.fetchall()
    row =  rows[0][0]
    return row

def drop_index(conn):
    cur = conn.cursor()
    cur.execute(drop_index_sql)
    conn.commit()
    
def update_simulation_rows(conn,data):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute(update_simulations_fk_block_id_sql,data)
    conn.commit()

def create_block(conn, id):
    cur = conn.cursor()
    cur.execute(create_new_block_sql,(id, str(f'block_{id}')))
    conn.commit()
    
def create_index_on_fk_blocks_id(conn):
    cur = conn.cursor()
    cur.execute(create_index_sql)
    conn.commit()

                                    
if __name__=="__main__":
    conn = create_tables.create_connection(create_tables.db_file)
    number_of_rows = find_highest_id(conn)
    block_size=10

    #Delete index on start
    drop_index(conn)
    
    mapping_between_block_and_simulation = []
    block_max= int(math.ceil(number_of_rows/block_size))
    current_simulation=0
    for block_id in range(1,block_max+1):
        end_simulation=current_simulation+block_size
        mapping_between_block_and_simulation.append((block_id,current_simulation,end_simulation))
        current_simulation = end_simulation
    
    for mapping in mapping_between_block_and_simulation:
        block_id=mapping[0]
        create_block(conn,block_id)
        update_simulation_rows(conn,mapping)    
    create_index_on_fk_blocks_id(conn)
