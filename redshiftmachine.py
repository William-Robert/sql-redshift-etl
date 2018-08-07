import io
import sys
import pprint
import pyodbc
import psycopg2
import boto3
import time
import json
from pandas import DataFrame
from contextlib import contextmanager

#context managers for db machines
from contxfunctions import open_redshift_db_connection
from con_defs import redshift_params

def main():
    print "I AM redshiftmachine MAIN()"


    ##############################################
    #The name of the src_db was hardcoded in
    #method 'copy_s3_to_staging'. If you're gonna run this,
    #you must change it. 
    ##############################################


class redshiftmachine():

    con_params = {}

    def __init__(self, redshift_params):
        self.con_params = redshift_params

    def startup_procedures(self):
        '''
        This runs sanity checks for redshift. I still need
        to add some functionality here...
        '''

        print "RUNNING redshiftmachine STARTUP PROCEDURES"
        if self.redshift_test(self.con_params):
            return True
        else:
            return False

    def redshift_delsert(self, table, pk, column_name_list):

        '''
        TODO: Design has changed, Instead of retrieving column name list
        and primary key from config folder in s3, I'm now quering that info
        directly from the source database. That means that I now need to 
        make space in the redshift instantiation for this. 
        '''

        con_params = self.con_params 
        with open_redshift_db_connection(con_params) as c:
            #Delete rows that were deleted or updated
            command = '''DELETE FROM '''+table+'''
            USING '''+table+'''_staging a
            WHERE '''+table+'''.'''+pk+'''=a.'''+pk+''' AND (__$operation LIKE '%4%' OR __$operation LIKE '%1%');'''

            c.execute(command)
            ## Insert rows to redshift prod  that were inserted or updated in src
            res = column_name_list[0].lower()
            for i in column_name_list[1:]:
                res += ','+i.lower()

            command = '''INSERT INTO '''+table.upper()+''' ('''+res+''')
            SELECT '''+res+''' FROM '''+table.upper()+'''_staging s
            WHERE __$operation LIKE '%4%' OR __$operation LIKE '%2';'''
            #print command
            c.execute(command)
        return True

    def copy_s3_to_staging(self, table_name):

        '''
        Takes connection parameters and a table name. 
        Copies that file in s3 to a staging table in redshift.
        '''

        redshift_params = self.con_params
        ##############################################
        ##############################################
        #I should point out that the databse name and
        #IAM role was hardcoded here. That's garbage. 
        ##############################################
        ##############################################

        with open_redshift_db_connection(redshift_params) as c:
            try:
                command = """COPY """+table_name+"""_staging
                    FROM 's3://ichain-sync-machine/"""+'DEAHINPHSDB'+'/'+table_name+'/'+table_name+"""-changes.csv'
                    IAM_ROLE 'arn:aws:iam::265991248033:role/myRedShiftforKin'
                    delimiter ','
                    IGNOREHEADER 1 removequotes emptyasnull blanksasnull maxerror 1
                    COMPUPDATE OFF STATUPDATE OFF;
                    """
                #print command
                c.execute(command)
            except Exception as e:
               #print (e)
               return False
        return True

    def truncate_redshift_staging_table(self, table_name):

        '''
        This deletes the rows in a STAGING table without deleting the columnn names.
        '''

        con_params = self.con_params
        command = ("""Truncate """+table_name+"""_staging;""")
        with open_redshift_db_connection(con_params) as c:
            try:
                c.execute(command)
            except Exception as e:
                print (e)
                return False
        return True

    def truncate_redshift_table(self, table_name):

        '''
        This deletes the rows in a table without deleting the columnn names.
        '''

        con_params = self.con_params
        command = ("""Truncate """+table_name+""";""")

        with open_redshift_db_connection(con_params) as c:
            try:
                c.execute(command)
            except Exception as e:
               print (e)
               return False
        return True

    def analyze_redshift_db(self, table = None):
        
        '''
        From AWS documentation:
        'Updates table statistics for use by the query planner.'
        SEE: https://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html
        
        Default is to analyze ALL tables. If you supply this method with a 
        table name, only that table will be analyzed.
        '''

        con_params = self.con_params

        if table:
            command = 'analyze'+table+';'
        else:
            command = 'analyze verbose;'

        with open_redshift_db_connection(con_params) as c:
            try:
                c.execute(command)
            except Exception as e:
               print (e)
               return False
        return True

    def vacuum_redshift_db(self, table = None):
        '''
        From AWS documentation:
        'Reclaims space and resorts rows in either a specified 
        table or all tables in the current database.'
        SEE: https://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html
        
        Default is to vacuum ALL tables. If you supply this method with a 
        table name, only that table will be vacuumed
        '''
        con_params = self.con_params
        if table:
            command = 'vacuum '+table+';'
        else:
            command = 'vacuum;'
        with open_redshift_db_connection(con_params) as c:
            try:
                c.execute(command)
            except Exception as e:
               print (e)
               return False

        return True

    def redshift_test(self, rs_params):
        '''
        This is a simple test of the redshift db. Prints 10 rows from a toytable in 
        the redshift dev database. 
        '''
        with open_redshift_db_connection(rs_params) as c:
            c.execute("SELECT * FROM toytable LIMIT 10;")
            rows = c.fetchall()
            for i in rows:
                print i
        return True


#############################################################################
#############################################################################
#############################################################################
#            Basically everything below this line is useless
#############################################################################
#############################################################################
#############################################################################

def redshift_task_runner(self):
    table_names_list = self.src_db.get_table_names()
    for table in table_names_list:
        column_names_list= self.src_db.get_column_names(table)
        pk = self.src_db.get_pk(table)

        self.copy_s3_to_staging(table)
        self.redshift_delsert(table, pk, column_name_list)
        self.truncate_redshift_staging_table(table)

def del_rs_rows(con_params, table_name):
    '''
    This will delete rows from the staging table in redshift to the proper table
    command = """
    DELETE FROM """++"""
    USING """++""""s"""+
    """
    Where"""++"""   """"
    AND (__$operation = '3' OR __$operation = '4');
    """
    '''
    with open_redshift_db_connection(redshift_params) as c:
        try:
            c.execute(command)
        except Exception as e:
           print (e)
    return

def insert_rs_rows(con_params, table_name):
    '''
    This will insert rows from the staging table in redshift to the proper table
    command = """
    DELETE FROM """++"""
    USING """++""""s"""+
    AND (__$operation = '3' OR __$operation = '4');
    AND (__$operation = '3' OR __$operation = '4');
    '''
    with open_redshift_db_connection(redshift_params) as c:
        try:
            c.execute(command)
        except Exception as e:
           print (e)
    return c.fetchall()

def test3(redshift_params):
    res = 'yis'
    print res
    with open_redshift_db_connection(redshift_params) as c:
        c.execute("""SELECT
                DISTINCT tablename
                FROM PG_TABLE_DEF
                WHERE schemaname = 'public';""")
        if c.fetchall():
            res = c.fetchall()
            for i in res:
                print i
                print i
        print res
        print "okay"
        return res

def test2(redshift_params):
    res = 'yis'
    with open_redshift_db_connection(redshift_params) as c:
        c.execute("""SELECT * FROM DEAA0_staging;""")
        if c.fetchall():
            res = c.fetchall()
        print res
        return res

def test(redshift_params):
    '''
    This is a simple test of the redshift db. Prints 10 rows from a toytable in 
    the redshift dev database. 
    '''
    df = object
    cols = object
    with open_redshift_db_connection(redshift_params) as c:
        c.execute("SELECT * FROM toytable LIMIT 10;")
        print c.fetchall()
        '''
        df = DataFrame(c.fetchall())
        cols = [col[0] for col in c.description]
        df = df.to_csv(sep = ',')
        '''
    return df, cols

def create_staging_table(redshift_params):
    '''
    '''
    with open_redshift_db_connection(redshift_params) as c:
        c.execute("CREATE TABLE sometable")
        rows = c.fetchall()
        for i in rows:
            print i
    return

if __name__ == "__main__":
    main()
