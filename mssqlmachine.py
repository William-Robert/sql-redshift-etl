import tempfile
import io
from contextlib import contextmanager
import sys
import pprint
import pyodbc
import psycopg2
import boto3
import time
import json
import csv
from pandas import DataFrame

from contxfunctions import open_sql_db_connection
from con_defs import sql_params

def main():
    '''
    this main is just for testing methods.
    '''
    print "I am mssqlmachine MAIN()"

class mssqlMachine():
    con_params = {}
    def __init__(self, con_params):
        '''
        It may be the case that I should take the time during __init__() to fetch
        things like 'tables names', 'column names' for each table, and primary keys. 
        Not sure I'll know until it all comes together.
        '''
        self.client = boto3.client('s3')
        self.con_params = con_params
        self.end_time_lsn = self.get_sql_end_lsn()
        self.table_names_list = self.get_table_names()
        self.tables_with_pks = self.get_tables_with_pks() 

    def startup_procedures(self):
        '''
        These are the start up procedures for ms sql db.
        -Check that CDC is enabled on all tables. If not, enable it. 
        !!!!!!!IMPORTANT!!!!!!!!
        The most important thing this method does is:
        1. Enable CDC on the ENTIRE SQL database.
        2. Enable CDC on EACH table within the db.

        This method should do these things automatically but there are
        a few caveats, ex. these commands need to be ran as admin.
        For this reason, it may be necessary to manually execute the commands
        that perform those operations. 
        '''
        print "RUNNING MSSQL STARTUP PROCEDURES"
        with open_sql_db_connection(self.con_params) as cursor:
            if not self.check_CDC_on_user_tables():
                print "CDC WAS NOT ENABLED ON ALL TABLES, ATTEMPTING TO ENABLE"
                if self.enable_CDC_on_tables():
                    print "CDC ENABLE WAS SUCCESSFUL"
                else:
                    #RAISE AIRFLOW EXCEPTION!!!!
                    print "CDC ENABLE FAILED"
            else:
                #RETURN TRUE FOR AIRFLOW!!!!!
                print "CDC IS PROPERLY ENABLED"
        return

    def table_has_changes(self, table_name):
        '''
        Takes a table name as a string, returns boolean if table has unloaded changes
        '''
        from_lsn = self.get_min_lsn(table_name)
        to_lsn = self.end_time_lsn

        with open_sql_db_connection(self.con_params) as cursor:
            command = """
            DECLARE @from_lsn binary(10), @to_lsn binary(10);   
            SET @from_lsn ="""+str(from_lsn)+""";
            SET @to_lsn ="""+str(to_lsn)+"""; 
            SELECT COUNT(*) FROM cdc.fn_cdc_get_net_changes_dbo_"""+table_name+"""(@from_lsn, @to_lsn, 'all');"""

            cursor.execute(command)
            rows = cursor.fetchone()
            return list(rows)[0]

    def nice_get_changes(self, table_name):
        '''
        Takes a tables name as a string, returns a generator object that allows you
        to only pull in 30,000 rows of data into memory at a time.
        This method streams data using the 'smart_open' library. 
        '''
        
        from_lsn = self.get_min_lsn(table_name)
        to_lsn = self.end_time_lsn

        with open_sql_db_connection(self.con_params) as cursor:
            command = """
            DECLARE @from_lsn binary(10), @to_lsn binary(10);   
            SET @from_lsn ="""+str(from_lsn)+""";
            SET @to_lsn ="""+str(to_lsn)+"""; 
            SELECT * FROM cdc.fn_cdc_get_net_changes_dbo_"""+table_name+"""(@from_lsn, @to_lsn, 'all');"""
            ###################################################
            #This command overrides in order to select 
            #all, use this to load all data from a table
            #command = """ SELECT * FROM """+table_name+""";"""
            ###################################################
            cursor.execute(command)
            while True:
                rows = cursor.fetchmany(30000)
                x = cursor.description
                if not rows:
                    break 
                else:  
                    for v in x:
                        print list(v)[0]
                    for row in rows:
                        for i in range(len(row)):
                            if isinstance(row[i], unicode):
                                row[i] = row[i].encode('utf-8')
                                print row[i]
                    for row in rows:
                        row[0] = None

                    temp =  DataFrame(rows).to_csv(  sep ="|")
                    buff = io.StringIO()
                    buff.write(temp.decode('UTF-8'))
                    buff2 = io.BytesIO(buff.getvalue().encode())
                    yield buff2

    def get_table_names(self):

        '''
        Returns a list of all the tables for a given db which
        were NOT shipped with ms sql. In other words, these 
        tables were created by user. 
        '''

        table_names_list = []

        with open_sql_db_connection(self.con_params) as cursor:
            command = "SELECT * FROM sys.tables WHERE is_ms_shipped = 0;"
            cursor.execute(command)
            rows = cursor.fetchall()
            for row in rows:
                table_names_list.append(row[0])
        return table_names_list

    def get_column_names(self, tablename):
        
        '''
        Takes a table name and returns a 2d array where each
        row contains info each particular column for a given table.

        I should probably change this to 'get_column_data', since you 
        have to parse the result to actually get the names of
        each column.
        '''
        
        command = """select *
        from INFORMATION_SCHEMA.COLUMNS
        where TABLE_NAME='"""+tablename+"""';"""
        column_names_dict = {}
        
        with open_sql_db_connection(self.con_params) as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
            column_names_dict = {}
            return rows
        
    def get_tables_with_pks(self):
        '''
        Returns a list of tables with primary keys. 

        NOTE: This is a pretty sloppy way of doing this. I should probably address it.
        '''
        result = []
        for table in self.table_names_list:
            if self.get_table_primary_key(table):
                result.append(table)
        return result

    def get_table_primary_key(self, tablename):
        '''
        Takes table name, returns the primary key(s) for that table.
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            command = """exec sp_pkeys '"""+tablename+"""';"""
            cursor.execute(command)
            result = cursor.fetchone()
            if result:
                return result[3]
            else:
                return None

        
    def clean_cdc_table(self, table):
        '''
        This is used to delete the changes logged in the CDC table for a given table. 

        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        !!!!!!!!!!!IMPORTANT!!!!!!!!!!!!!!!
        !If you're debugging this method,!!
        !consider the '@threshhold' value.!
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        TODO:This should be returning a boolean indicating the methods success/failur.
        '''
        command = '''DECLARE @Last_LSN VARBINARY(10) ='''+self.end_time_lsn+'''
        EXEC sys.sp_cdc_cleanup_change_table
          @capture_instance = 'dbo_'''+table+'''',
            @low_water_mark = @Last_LSN,
              @threshold = 100000'''
        with open_sql_db_connection(self.con_params) as cursor:
            try:
                cursor.execute(command)
            except:
                pass
            
            #rows = cursor.fetchone()
            return 

    def get_min_lsn(self, table):
        '''
        Takes a table_name and returns the min lsn for that tables cdc-table.
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            command = """SELECT sys.fn_cdc_get_min_lsn('dbo_"""+table+"""');"""
            cursor.execute(command)
            rows = cursor.fetchone()
            return self.lsn_bytearray_converter(str(rows[0]))

    def enable_CDC_on_db(self):
        '''
        This enables CDC on the entire db.

        NOTES:
        -THIS NEEDS TO RETURN AN ERROR!!!!!
        -This operation is handled differently when the SQL instance is hosted on AWS rds,
        so consider this when debugging.
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            command = """exec msdb.dbo.rds_cdc_enable_db """+self.con_params['database']+"""';"""
            cursor.execute(command)
        return True

    def enable_CDC_on_tables(self):
        '''
        Enables Change_Data_Capture on all user created
        tables in db
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            table_names_list = self.get_table_names()
            cdc_enabled_tables_list = self.get_tables_tracked_by_CDC()
            #this is annoying but it needs to 
            '''
            command = "exec msdb.dbo.rds_cdc_enable_db 'DEAHINPH'" 
            cursor.execute(command)
            '''
            for table_name in table_names_list:
                if not (table_name in cdc_enabled_tables_list):
                    print "Table found not being tracked: ", table_name
                    command = """exec sys.sp_cdc_enable_table   
                       @source_schema           = N'dbo'
                       ,  @source_name             = N'"""+table_name+"""'
                       ,  @role_name               = N'NULL';"""
                    cursor.execute(command)
                else:
                    print table_name, " IS BEING TRACKED BY CDC"
        return True

    def get_tables_tracked_by_CDC(self):
        '''
        Returns all the tables for a given db which
        were NOT shipped with ms sql. In other words, these 
        tables were created by user. 
        '''
        command = "SELECT * FROM sys.tables WHERE is_tracked_by_cdc = 1;"
        with open_sql_db_connection(self.con_params) as cursor:
            cursor.execute(command)
            rows = cursor.fetchall()
            table_names = []
            for row in rows:
                table_names.append(row[0])
            return table_names

    def check_CDC_on_user_tables(self):
        '''
        Checks that CDC is enabled on all user tables
        '''
        #result = bool
        cdc_tables_list = self.get_tables_tracked_by_CDC()
        user_tables_list = self.get_table_names()
        result = (cdc_tables_list == user_tables_list)
        return result

    def lsn_bytearray_converter(self, time_byte_array):
        '''
        MS SQL returns these obnoxious bytearray things as LSN numbers.
        This converts them into a string so it can be stored in a csv
        '''
        result = '0x'
        result +=str(time_byte_array).encode('hex')
        return result

    def get_sql_end_lsn(self):
        '''
        Returns an end date. This will be used to clean up the 
        CDC table after the net changes are uploaded to s3.
        '''
        command = """DECLARE @end_time datetime, @to_lsn binary(10);  
        SET @end_time = GETDATE();
        SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time)
        SELECT @to_lsn;"""
        result = '0x'
        with open_sql_db_connection(self.con_params) as cursor:
            cursor.execute(command)
            end_time = cursor.fetchone()
            result +=str(end_time[0]).encode('hex')
            return result

    def mssql_change_test(self):
        '''
        This function is used to create a change in the database which is 
        obviously necessary to populate the CDC table for a give table. 
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            command = """SELECT TOP 10  * FROM DEAA0
            WHERE City LIKE 'A%'
            ORDER BY DEANumber"""
            cursor.execute(command)
            rows = cursor.fetchone()
            print rows
            if rows:
                command = """UPDATE DEAA0
                SET City = 'Las Gatos'
                WHERE ID = """+str(rows[0])+';'
                cursor.execute(command)
        return

    def sam(self):

        '''
        '''

        with open_sql_db_connection(self.con_params) as cursor:
            command = """SELECT TOP 10  * FROM DEAA0
            WHERE City LIKE 'A%'
            ORDER BY DEANumber"""
            cursor.execute(command)
            rows = cursor.fetchall()
            res = DataFrame(rows).to_csv(  sep ="|")
            for row in rows:
                '''
                for i in range(len(row)):
                    if isinstance(row[i], unicode):
                        row[i] = row[i].encode('utf-8')
                        print row[i]
                '''
                print list(row)
            print res
        return

########################################################################
########################################################################
########################################################################
#All the methods below this line are not being used anymore, I just don't 
#have the strength to delete them yet...
########################################################################
########################################################################
########################################################################

    def get_net_changes_sql(self, table_name):
        '''
        Return all the next changes for a given table.
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            #time  = self.get_sql_end_date()
            command = """
            DECLARE @begin_time datetime, @end_time datetime, @from_lsn binary(10), @to_lsn binary(10);   
            SET @begin_time = 0;
            SET @end_time = GETDATE();
            SET @from_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',@begin_time);
            SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);  
            SELECT * FROM cdc.fn_cdc_get_net_changes_dbo_"""+table_name+"""(@from_lsn, @to_lsn, 'all');"""
            cursor.execute(command)
            rows = cursor.fetchall()

            cols = [str(col[0]) for col in cursor.description]
            res = ''
            if rows:
                res = DataFrame(rows).to_csv(  sep ="|")
            return rows, cols

    def get_sql_end_lsn2(self):
        '''
        DEPRECATED! Leaving him around just in case..

        Returns an end date. This will be used to clean up the 
        CDC table after the net changes are upoaded to s3.
        '''
        command = """DECLARE @end_time datetime;  
        SET @end_time = GETDATE();
        SELECT @end_time;"""
        with open_sql_db_connection(self.con_params) as cursor:
            cursor.execute(command)
            end_time = cursor.fetchone()
        return end_time[0]

    def get_net_changes_sql2(self, table_name):
        '''
        Return all the next changes for a given table.
        '''
        from_lsn = self.get_min_lsn(table_name)
        to_lsn = self.end_time_lsn

        with open_sql_db_connection(self.con_params) as cursor:
            command = """
            DECLARE @from_lsn binary(10), @to_lsn binary(10);   
            SET @from_lsn ="""+str(from_lsn)+""";
            SET @to_lsn ="""+str(to_lsn)+"""; 
            SELECT * FROM cdc.fn_cdc_get_net_changes_dbo_"""+table_name+"""(@from_lsn, @to_lsn, 'all');"""
            cursor.execute(command)
            rows = cursor.fetchall()
            cols = [str(col[0]) for col in cursor.description]
            res = ''
            if rows:
                res = DataFrame(rows).to_csv(  sep ="|")
            return rows, cols

    def load_changes(self, changes, col_names, table):
        '''
        DEPRECATED by 'nice_get_changes()'!!!
        This method is used for loading chnages from the SQL db into s3. 
        I'm keeping it here because I may end up using it though because
        nice_get_changes is having some issues...
        '''
        with open('tmp.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            for row in changes:
                #row[0] = self.lsn_bytearray_converter(row[0])
                #row[0] = None
                writer.writerow(row)

        with open('tmp.csv', 'r') as c:
            print "THIS IS RESPONSE: ", self.s3_put_object(data = c, table_name = table, file_name = table+'-changes.csv' )
        return True

    def s3_put_object(self, data = '', table_name = None, file_name = ''):
        '''
        Wrapper for boto3, Puts an object into s3 bucket. 
        '''

        bucket_name = 'ichain-sync-machine'
        database = self.con_params['database']
        key_string = self.s3_key_builder(database, table_name, file_name)
        response = self.client.put_object(Body = data, Bucket = bucket_name, Key = key_string)

        return response

    def s3_key_builder(self, database, table_name = '', file_name = ''):
        '''
     Builds the Key string for creating an s3 object
     Default is no filename (for building a folder object key)
        '''
        key_string = database+'/'
        if not table_name == '':
            key_string +=table_name+"/"
        if not file_name == '':
            key_string += file_name
        return key_string


    def insert(self):
        '''
        this looks like a nonsense method. Not sure what I was thinking here...
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            command = """INSERT  * FROM DEAA0
            WHERE City LIKE 'Las A%'
            ORDER BY DEANumber"""
            cursor.execute(command)
            rows = cursor.fetchone()



    def get_table_changes(self, tableName):
        '''
        Returns all the changes for a given table
        '''
        with open_sql_db_connection(self.con_params) as cursor:
            try:
                command = "SELECT * FROM cdc.dbo_"+tableName+"_CT;"
                cursor.execute(command)
                rows = cursor.fetchall()
                return rows
            except:
                #this should be change to a log statement
                print "table didn't exist...maybe"
                return

if __name__ == "__main__":
    main()
