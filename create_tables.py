import sqlite3
import traceback
db_file ='sqlite_test.db'
sql_create_blocks_table = """ CREATE TABLE IF NOT EXISTS blocks (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    begin_date text,
                                    end_date text
                                ); """

sql_create_simulations_table = """CREATE TABLE IF NOT EXISTS simulations (
                                id integer PRIMARY KEY,
                                directory text NOT NULL,
                                distance float NOT NULL,
                                start_time text,
                                end_time text,
                                completion_status int,
                                fk_blocks_id INTEGER,
                                FOREIGN KEY (fk_blocks_id) REFERENCES blocks (id)
                            );"""

sql_drop_table_simulations = """DROP TABLE IF EXISTS simulations;"""
sql_drop_table_blocks = """DROP TABLE IF EXISTS blocks;"""

def create_connection(dbf=db_file,timeout=5.0,isolation_level=None):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        if isolation_level is not None:
            conn = sqlite3.connect(dbf,timeout, isolation_level)
        else:
            conn = sqlite3.connect(dbf,timeout)
    except Exception as e:
        print(e)
    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def drop_table(conn, drop_table):

    try:
        c = conn.cursor()
        c.execute(drop_table)
    except Exception as e:
        print(e)
        
def DoesTableExist(conn, table_name):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))

    rows = cur.fetchall()

    rows = [i[0] for i in rows]
    print(rows)
    for row in rows:
        print(row)

    return rows

def insert_into_simulations(conn, records):
    if conn == None:
        print("No valid connection")

    cur = conn.cursor()
    cur.executemany("insert into simulations(id, directory,distance) values(?,?,?);", records)
    conn.commit()


if __name__ == '__main__':
    try:
        tables_to_create= [("blocks",sql_create_blocks_table), ("simulations",sql_create_simulations_table)]
        conn = create_connection(r"sqlite_test.db")
        if conn is not None:
            drop_table(conn,sql_drop_table_simulations)
            drop_table(conn,sql_drop_table_blocks)
            for table in tables_to_create:
                try:
                    create_table(conn, table[1])
                except Exception as e:
                    print(f"Unable to create exception due to {e}")

                rows=DoesTableExist(conn,table[0])
                if table[0] in rows:
                     print(r"Table {table[0]} created successfully")
                else:
                     print(r"Table {table[0]} not created")

        else:
            print("Error! cannot open connection")
        records = [(1,"/firstdir",23.4),(2,"/seconddir",24)]
        insert_into_simulations(conn, records)
    except Exception as e:
        print(f"Unknown failure {e}")
        traceback.print_exc()
    finally:
        conn.close()
        
