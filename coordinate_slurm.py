from multiprocessing import Pool
from get_next_block import get_simulations,get_simulations_for_a_block,get_next_block,update_end_date_for_a_block
import create_tables
from set_simulation_times import set_simulation_end_time, set_simulation_start_time
from simulations import Simulation
from os import system
from datetime import datetime
import threading
from random import uniform
import traceback



def run_each_simulation(sim):
    test=None
    try:
        print(f"Here is the {sim}")
        
        thread_name = threading.get_native_id()
        print(f'The thread is: {thread_name}')
        sim.start_time = datetime.now()
        delay = uniform(0.5,5)
        execute_string = f'sleep {delay} && echo "{sim.directory}" >> help'
        print(f'String to be executed is: {execute_string}')
        sim.completion_status = system(execute_string)
        print(f'Test return value is {sim.completion_status}')
        sim.end_time = datetime.now()
    except Exception as e:
        print(e)
        traceback.print_exc()
        sim.completion_status = 999
    return sim

def run_each_block():
    try:
        next_block = False
        with create_tables.create_connection(create_tables.db_file,timeout=30, isolation_level=None) as conn:
            next_block=get_next_block(conn)

            if next_block is False:
                print('Is next_block false')
                return False

        with create_tables.create_connection() as sim_conn:
            sim= get_simulations_for_a_block(sim_conn,(next_block,))    

            print(sim)
            if sim:
                print(f'Block Id is {next_block}, number of sim is {len(sim)}')
                p =  Pool(processes = len(sim))
                async_result = p.map_async(run_each_simulation, sim)
                p.close()
                p.join()
                #updates all the values of the simulation in the database, but as part of a single thread.
                print(async_result)
                return_value =  [r.update() for r in async_result._value]
            else:
                return False
       
        update_end_date_for_a_block(next_block)
    except Exception as e :
        print(e)
        return False 


if __name__ == '__main__':
    run_each_block()
