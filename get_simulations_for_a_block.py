import create_tables
import sqlite3                                                                                                      
from pathlib import Path

get_simulations_for_a_block_sql = """select * from simulations where fk_blocks_id=?;"""

def get_simulations_for_a_block(conn,data):
    cur = conn.cursor()
    cur.execute(get_simulations_for_a_block_sql, data)
    rows = cur.fetchall()
    
    return rows
                                    
if __name__=="__main__":
    conn = create_tables.create_connection(create_tables.db_file)
    data =4
    rows= get_simulations_for_a_block(conn,(data,))    
    print(rows)
