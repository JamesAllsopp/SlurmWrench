from multiprocessing import Pool
from get_next_block import get_simulations,get_simulations_for_a_block,get_next_block,update_end_date_for_a_block
import create_tables
from set_simulation_times import set_simulation_end_time, set_simulation_start_time
from os import system
import threading
from random import uniform
import traceback


def run_each_simulation(simulation_tuple):
    try:
        print(r"Here is the {simulation_tuple}")
        simulation_id = simulation_tuple[0]
        simulation_dir = simulation_tuple[1]

        conn = create_tables.create_connection(create_tables.db_file)
        thread_name = threading.get_native_id()
        print(f'The thread is: {thread_name}')
        set_simulation_start_time(simulation_id)
        delay = uniform(0.5,5)
        execute_string = f'sleep {delay} && echo "{simulation_dir}" >> help'
        print(f'String to be executed is: {execute_string}')
        test = system(execute_string)
        print(f'Test return value is {test}')
        set_simulation_end_time(simulation_id, test)
        return test
    except Exception as e:
        print(e)
        traceback.print_exc()

def run_each_block():
    try:
        conn = create_tables.create_connection(create_tables.db_file,timeout=30, isolation_level=None)
        next_block=get_next_block(conn)

        if next_block is False:
            print('Is next_block false')
            return False

        new_conn = create_tables.create_connection(create_tables.db_file)
        rows= get_simulations_for_a_block(new_conn,(next_block,))    

        print(rows)
        if rows:
            print(f'Block Id is {next_block}')
            print(rows[0])
            print(type(rows[0]))
            output =[run_each_simulation(row) for row in rows]
            print(output)
            # This doesn't work as you can't create children of children in multiprocessing
            #p =  Pool(processes = len(rows))
            #async_result = p.map_async(run_each_simulation, rows)
            #p.close()
            #p.join()
            update_end_date_for_a_block(new_conn, next_block)
    except Exception as e :
        print(e)
        return False 
    return True

if __name__ == '__main__':
    results=[]
    with Pool() as pool:
        future_results = [pool.apply_async(run_each_block) for i in range(5)]
        results = [f.get() for f in future_results]
    print(results)
