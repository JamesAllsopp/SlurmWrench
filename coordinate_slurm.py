from multiprocessing import Pool
from get_next_block import get_simulations,get_simulations_for_a_block,get_next_block,update_end_date_for_a_block
import create_tables
from set_simulation_times import set_simulation_end_time, set_simulation_start_time
from os import system
import threading
from random import uniform
import traceback



def run_each_simulation(simulation_tuple):
    test=None
    try:
        print(f"Here is the {simulation_tuple}")
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
    except Exception as e:
        print(e)
        traceback.print_exc()
    return test

def run_each_block():
    try:
        next_block = False
        with create_tables.create_connection(create_tables.db_file,timeout=30, isolation_level=None) as conn:
            next_block=get_next_block(conn)

            if next_block is False:
                print('Is next_block false')
                return False

        with create_tables.create_connection() as sim_conn:
            rows= get_simulations_for_a_block(sim_conn,(next_block,))    

            print(rows)
            if rows:
                print(f'Block Id is {next_block}, number of rows is {len(rows)}')
                p =  Pool(processes = len(rows))
                async_result = p.map_async(run_each_simulation, rows)
                p.close()
                p.join()
                print(async_result)
            else:
                return False
       
        update_end_date_for_a_block(next_block)
    except Exception as e :
        print(e)
        return False 


if __name__ == '__main__':
    run_each_block()
