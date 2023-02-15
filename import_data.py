import create_tables
import sqlite3                                                                                                      
from pathlib import Path                                                                                                     
                                                                                                                    
def read_file(filename:str)-> list:                                                                              
    path = Path(filename)
    if not path.is_absolute():
        path=path.resolve()

    print(path)
    with open(path,'r') as input:                                                                               
        lines = [tuple(line.rstrip().split()) for line in input]
    return lines

def delete_all_from_simulations(conn)->bool:
    if conn == None:
        print("No valid connection")
        return False

    cur = conn.cursor()
    cur.execute("delete from simulations;")
    conn.commit()
    return True
    
if __name__=="__main__":
    lines = read_file('test_data.tsv')
    print(lines)
    conn = create_tables.create_connection('sqlite_test.db')
    delete_all_from_simulations(conn)
    create_tables.insert_into_simulations(conn,lines)
    
