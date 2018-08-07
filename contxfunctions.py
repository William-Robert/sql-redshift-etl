from contextlib import contextmanager
import sys
import pyodbc
import psycopg2
import boto3

#connection paramters for db machines


@contextmanager
def open_redshift_db_connection(redshift_params):
    '''
    Context monitor for assuring the connections to the database are closed
    '''
    redshift_conn_string = 'dbname='+redshift_params['database']+' user='+redshift_params['username']+' host='+redshift_params['server']+' password='+redshift_params['password']+' port='+redshift_params['port']

    con = psycopg2.connect(redshift_conn_string)
    cursor = con.cursor()
    try:
        yield cursor
    except Exception as e:
        print "There was some error in the connection:"
        print (e)
        con.rollback()
    finally:
        con.commit()
        con.close()

@contextmanager
def open_sql_db_connection(sql_params, commit=True):
    '''
    Context monitor for assuring the connections to the database are closed
    '''
    sql_conn_string = 'DRIVER={ODBC Driver 13 for SQL Server};SERVER='+sql_params['server']+';PORT='+sql_params['port']+';DATABASE='+sql_params['database']+';UID='+sql_params['username']+';PWD='+ sql_params['password']

    cnxn = pyodbc.connect(sql_conn_string)
    #cnxn.setencoding(str,encoding=  'latin1')
    cursor = cnxn.cursor()
    try:
        yield cursor
    except pyodbc.DatabaseError as err:
            #this error handling isn't working correctly. still need to figure it out...
            #error, = err.args
            #sys.stderr.write(error.message)
            cursor.execute("ROLLBACK")
            raise err
    else:
        if commit:
            cursor.execute("COMMIT")
        else:
            cursor.execute("ROLLBACK")
    finally:
        cnxn.close()






