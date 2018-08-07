import sys
import io
import smart_open
from contextlib import contextmanager
import sys
import pprint
import pyodbc
import psycopg2
import boto3
import time
import json
import tempfile
import csv
import warnings

from mssqlmachine import mssqlMachine
from redshiftmachine import redshiftmachine

from con_defs import sql_params as sp
from con_defs import redshift_params as rs

from contxfunctions import open_redshift_db_connection

sql_params = sp
redshift_params = rs

def main():
    '''
    this main is just to test different methods. 
    '''
    sql = mssqlMachine(sp)
    redshift = redshiftmachine(rs)
    s = s3machine(sql, redshift)
    #s.src_db.sam()
    
    r, c= s.src_db.get_net_changes_sql2('DEAA0')
    s.src_db.load_changes(r, c, 'DEAA0')
    '''
    Bro don't forget you that you're not deleting the csv in s3...
    '''

    #s.src_runner()
    s.target_runner()
class s3machine:

    BUCKETNAME = 'sync-machine-bucket'
    src_con_params = {} 
    target_con_params = {} 
    src_table_names_list = []

    def __init__(self, src_dbmachine, target_dbmachine):
        self.client = boto3.client('s3')

        self.src_db = src_dbmachine
        self.src_database = self.src_db.con_params['database']
        self.src_con_params = self.src_db.con_params

        self.target_db = target_dbmachine
        self.target_database = self.target_db.con_params['database']
        self.target_con_params = self.target_db.con_params
        try:
            self.end_date = self.src_db.get_sql_end_date()
            self.end_lsn = self.src_db.get_sql_end_lsn()
        except:
            pass
        self.src_table_names_list = self.src_db.get_table_names()

    def startup_procedures(self):
        '''
        This runs startup proceducres for src and target db machines. 
        '''
        self.src_db.startup_procedures()
        self.target_db.startup_procedures()

        #look for changes.cvs objects not cleared from s3
        bucket_objs = self.list_bucket_objects()
        for obj in bucket_objs:
            if 'changes' in obj:
                print("WARNING!!!! FOUND UNLOADED FILE: ", obj)        
                self.target_runner()
        ######################################################
        #the below method is overriding all column datatypes...
        self.init_staging_tables()
        ######################################################
        return True

    def init_staging_tables(self):
        '''
        this is overriding all column datatypes...
        TODO: I should really be querying the source db and finding out
        the data types for each column instead of just setting them the way I am
        now. 

        TODO:I should also be looking at the staging table to make sure that there 
        are no more rows left to be delserted into produciton. 
        '''
        for table in self.src_db.tables_with_pks:
            columns_info_list = self.src_db.get_column_names(table)
            #Forgive me for I have hardcoded...
            cols_and_types = '__$start_lsn varchar(255), __$operation varchar(255), __$update_mask varchar(255)'
            #This is dumb, I'm overriding datatypes for all columns...
            for row in columns_info_list:
                cols_and_types += ',\n'+row[3]+' '+'varchar(255)'
            pk = self.src_db.get_table_primary_key(table)

            if not pk:
                pk = ''
            command = """CREATE TABLE """+table+"""_staging(
            """+cols_and_types+""",
            PRIMARY KEY ("""+str(pk)+""")\n);"""
            print command
            with open_redshift_db_connection(redshift_params) as c:
                c.execute(command)
        return True

    def init_rs_dummy_tables(self):
        '''
        This is just making an empty table in redshift to test the delsert command 
        since I haven't populated the redshift db with data yet. 
        '''
        for table in self.src_db.tables_with_pks:
            columns_info_list = self.src_db.get_column_names(table)
            cols_and_types = ''
            cols_and_types += columns_info_list[0][3]+' '+'varchar(255)'
            for row in columns_info_list[1:]:
                cols_and_types += ',\n'+row[3]+' '+'varchar(255)'
            pk = self.src_db.get_table_primary_key(table)
            if not pk:
                pk = ''
            command = """CREATE TABLE """+table.lower()+"""(
            """+cols_and_types+""",
            PRIMARY KEY ("""+str(pk)+""")\n);"""
            print command
            with open_redshift_db_connection(redshift_params) as c:
                c.execute(command)
        return True

    def target_runner(self):
        '''
        This will go to the target db(redshift) and delsert the data from 
        staging to the production server and then truncate the staging table. 
        '''
        for table_name in self.src_db.tables_with_pks:
            if self.target_db.copy_s3_to_staging(table_name):
                s3_key = self.src_database+'/'+table_name+'/'+table_name+'-changes.csv'
                #self.client.delete_object(Bucket = self.BUCKETNAME, Key = s3_key)
                pk = self.src_db.get_table_primary_key(table_name)
                column_name_list = self.src_db.get_column_names(table_name)
                cols = []
                for i in column_name_list:
                    cols.append(str(i[3]))
                self.target_db.redshift_delsert(table_name, pk, cols)
                #################################################################
                #!!!Important!!!
                #The below command should only run if above 'delsert' returns true
                self.target_db.truncate_redshift_staging_table(table_name)
                #################################################################
        return

    def src_runner(self):
        '''
        This handles pretty much all of the procesess to get and load changes into s3.

        Uses this python module for streaming to s3:
        https://github.com/RaRe-Technologies/smart_open/
        very important!!
        '''
        for table_name in self.src_db.tables_with_pks:
            print "working on: ",table_name
            num_of_rows = self.src_db.table_has_changes(table_name)
            if num_of_rows:
                s3_key = self.src_database+'/'+table_name+'/'+table_name+'-changes.csv'
                with smart_open.smart_open('s3://ichain-sync-machine/'+self.src_database+'/'+table_name+'/'+table_name+'-changes.csv', 'wb') as fout:
                    for chunk in self.src_db.nice_get_changes(table_name):
                        fout.write(chunk.read())
                '''
                The following methods will clear the CDC table. I have
                them commented out because if they run, I will need to make
                new changes before I can test again. 
                '''
                #if find_s3(s3_key):
                    #self.src_db.clean_cdc_table(table_name)
                #else:
                    #raise error/break
                '''
                The below commands were moved to the 'target_runner' method
                but I want to leave here, commented out, just incase..
                '''
                #self.target_db.copy_s3_to_staging(table_name)
                #self.client.delete_object(Bucket = self.BUCKETNAME, Key = s3_key)
        return True

    def list_bucket_objects(self, bucket_name = BUCKETNAME): 
        '''
        Returns a list of objects in a bucket. Default is the bucket name specified
        within the connection paramets. 
        '''
        bucket_objs_list = self.client.list_objects(Bucket = bucket_name)
        result = []
        for i in range(len(bucket_objs_list['Contents'])):
            result.append( bucket_objs_list['Contents'][i]['Key'])
        return result

    def s3_put_object(self, data = '', table_name = None, file_name = ''):
        '''
        Wrapper for boto3, Puts an object into s3 bucket. 
        '''
        bucket_name = self.BUCKETNAME
        database = self.database
        key_string = self.s3_key_builder(database, table_name, file_name)
        response = self.client.put_object(Body = data, Bucket = bucket_name, Key = key_string)
        return response

    def init_s3_for_db(self):
        '''
        Uses get_table_names to find all the table names for the database defined
        in con_params, then checks inside BUCKETNAME to see if they 
        are there, otherwiseit will create folders for each table name. 

        TO DO this should also define primary keys for each table, column 
        names, store those in s3 aswell as in the class variables. 
        '''
        if self.find_s3_bucket(self.BUCKETNAME):
            if not (self.table_names_list):
                self.table_names_list = self.db.get_table_names()
            bucket_objects_list = self.list_bucket_objects()
            for table_name in self.table_names_list:
                if not (self.s3_key_builder(database = self.database, table_name = table_name) in bucket_objects_list):
                    print "Could not find folder for :", table_name
                    print "Attempting to create folder for :", table_name
                    if self.s3_put_object(table_name = table_name):
                        print "File created"
        else:
            print "sync machine bucket doesn't exist"
            return False
        return True

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

    def list_s3_buckets(self):
        '''
        Returns an array of s3 bucket names
        '''
        client = self.client
        http_response = 404
        counter = 1
        while (http_response != 200 and counter < 5):
            response = client.list_buckets()
            http_response = response['ResponseMetadata']['HTTPStatusCode']
            print ("list_s3_buckets RESPONSE: "+ str(http_response)+ ", ATTEMPT: "+str(counter)) 
            if (http_response == 200):
                bucket_names_list = []
                for bucket in response['Buckets']:
                    bucket_names_list.append(bucket['Name'])
                return bucket_names_list
            counter += 1
            if (counter == 5):
                print ("""Function 'list_s3_buckets' Exceeded maximum number of attempts""")
        return False

    def find_s3_bucket(self, bucket_name = BUCKETNAME):
        '''
        Looks to see if s3 bucket exists and returns boolean.
        '''
        try:
            return (bucket_name in self.list_s3_buckets())
        except:
            print ("Some error in finding s3 bucket")
            pass
        return False

    def make_s3_bucket(self, bucket_name = BUCKETNAME, acl = 'private'):
        '''
        Makes an s3 bucket and then checks to make sure it's there. 
        Default is to make bucket private. 
        NOTE: AWS api sets location to us-east-1 by default AND will error if you
        try to explicitly set it as us-east-1, so leave it blank. 
        If you want to change the default location,
        inlcude parameter: 'CreateBucketConfiguration = {'LocationConstraint': 'us-west-1'}'
        within the below command: 'response = client.create_bucket(...)'
        '''
        client = self.client
        command_success = False
        if (self.find_s3_bucket(bucket_name)):
                print ('Bucket already exists')
                command_success =  True
        else:
            counter = 1
            while (command_success == False and counter < 5):
                try: 
                    response = client.create_bucket(ACL = acl, Bucket = bucket_name)
                    command_success = self.find_s3_bucket(bucket_name)
                except:
                    print "ERROR. Likely because bucket exists, but code shouldn't exec if it exists. May need to review this.."
                    counter = 5
                counter +=1
            if (counter == 5):
                print "make_s3_bucket aborted"
        return command_success


''' 
j= tempfile.TemporaryFile()
j.write(json.dumps((self.s3_column_dict_builder(table_name))))
j.seek(0)
self.s3_put_object(data = j.read(), table_name = table_name, file_name = (table_name+'-columns.txt'))
j.close()
'''

if __name__ == "__main__":
    main()
