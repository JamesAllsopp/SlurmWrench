import create_tables
import sqlite3
from datetime import datetime
from time import sleep
from random import uniform

update_simulations_start_time_sql = """update simulations set start_time=?, completion_status =1 where id=?;"""

update_simulations_end_time_sql = """update simulations set end_time=?, completion_status =? where id=?;"""

def set_simulation_start_time(id):
    data = (datetime.now(), id)
    __set_time(update_simulations_start_time_sql,data)

def set_simulation_end_time(id, status):
    data = (str(datetime.now()), status, id)
    __set_time(update_simulations_end_time_sql,data)

def __set_time(sql_command, data):
    retries=0
    while retries<5:
        try:
            with create_tables.create_connection() as conn:
                cur = conn.cursor()
                cur.execute("begin")
                cur.execute(sql_command, data)
                return
        except Exception as e:
            print(f"__set_time has failed with {sql_command}")
            print(e)
            sleep_time = uniform(0.1,4)
            print(f"Sleeping for {sleep_time}")
            sleep(sleep_time)
            retries+=1
            
    raise Exception(f"__set_time failed after {retries}")

                                    
if __name__=="__main__":
    with create_tables.create_connection() as conn:
        test_id = 1
        set_simulation_start_time( test_id)
        sleep(17)
        set_simulation_end_time(test_id)

