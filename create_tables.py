import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    '''
    This Function is used to Drop tables using SQL queries.
    SQL queries are imported from another file in a list type which contain all SQL statments requeried to perferm Dropping tables
    '''
    
    '''
    Parameters:
    cur : cursor object
        Enables the execution of PostgreSQL command in a database session.
        
    conn : connection object
        Handles the connection to a PostgreSQL database instance.
   '''
    
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    '''
    This Function is used to Create tables using SQL queries.
    SQL queries are imported from another file in a list type which contain all SQL statments requeried to perferm Creating tables
    '''
    
    '''
    Parameters:
    cur : cursor object
        Enables the execution of PostgreSQL command in a database session.
        
    conn : connection object
        Handles the connection to a PostgreSQL database instance.
   '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()