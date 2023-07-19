import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    '''
    This Function is used to load data to staging tables using SQL queries.
    SQL queries are imported from another file in a list type which contain all SQL statments requeried to perferm loading data
    '''
    
    '''
    Parameters:
    cur : cursor object
        Enables the execution of PostgreSQL command in a database session.
        
    conn : connection object
        Handles the connection to a PostgreSQL database instance.
   '''
    
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    '''
    This Function is used to insert data to dimensional tables from staging tables using SQL queries.
    SQL queries are imported from another file in a list type which contain all SQL statments requeried to perferm loading data
    '''
    
    '''
    Parameters:
    cur : cursor object
        Enables the execution of PostgreSQL command in a database session.
        
    conn : connection object
        Handles the connection to a PostgreSQL database instance.
   '''
    
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

   
    # Establish connection to  Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()