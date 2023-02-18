from create_tables import create_connection
from datetime import datetime
from dataclasses import dataclass
from random import uniform
from time import sleep

get_sql = """select * from simulations where id=?"""
   
@dataclass
class Simulation:
    id: int
    directory: str
    distance: float
    start_time:str
    end_time: str
    completion_status: int
    fk_blocks_id:int

    update_sql = """update simulations set directory=?, distance=?, start_time=?, end_time=?, completion_status=?, fk_blocks_id=? where id=?"""

 

    @staticmethod
    def get_simulation(id):
        retries=0
        while retries<5:
            try:
                with create_connection() as conn:
                    c=conn.cursor()
                    c.execute("begin")
                    print(get_sql)
                    c.execute(get_sql, (id,))
                    first_row = c.fetchone()
                    print(first_row)
                    if first_row is not None:
                        return Simulation(*first_row)
            except Exception as e :
                 print(e)
                 sleep_time = uniform(0.1,4)
                 print(f"Sleeping for {sleep_time}")
                 sleep(sleep_time)
                 retries+=1

        return false


    def get_update_tuple(self):
        return (self.directory, self.distance, self.start_time, self.end_time, self.completion_status, self.fk_blocks_id, self.id)
    
    def update(self):
        retries=0
        while retries<5:
            try:
                with create_connection() as conn:
                    print("updating simulation")
                    cur = conn.cursor()
                    cur.execute("begin")
                    cur.execute(self.update_sql, self.get_update_tuple())
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
    sim = Simulation.get_simulation(0)
    sim.start_time= datetime.now()
    sim.end_time= datetime.now()
    print(sim)
    sim.update()
