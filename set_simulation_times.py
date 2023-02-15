import create_tables
import sqlite3
from datetime import datetime
from time import sleep

update_simulations_start_time_sql = """update simulations set start_time=?, completion_status =1 where id=?;"""

update_simulations_end_time_sql = """update simulations set end_time=?, completion_status =? where id=?;"""

def set_simulation_start_time(conn,id):
    data = (datetime.now(), id)
    __set_time(conn,update_simulations_start_time_sql,data)

def set_simulation_end_time(conn,id):
    data = (str(datetime.now()), status, id)
    __set_time(conn,update_simulations_end_time_sql,data)

def __set_time(conn,sql_command, data):
    cur = conn.cursor()
    cur.execute(sql_command, data)
    conn.commit()

                                    
if __name__=="__main__":
    conn = create_tables.create_connection(create_tables.db_file)
    test_id = 1
    set_simulation_start_time(conn, test_id)
    sleep(17)
    set_simulation_end_time(conn, test_id)
    
