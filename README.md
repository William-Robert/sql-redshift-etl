# db_to_redshift

#### (aka ichain sync machine)
###### -William Hodges
------
### What does it do?
This program is designed to sync changes between source and target databases. It operates as an airflow dag with three tasks.
From a high level, the three tasks look like this:

1. Check connectivity to the source and target dabases using connection strings defined in 'con_defs.py'.
2. Connect to source database and get net changes for a given time period and write those changes to s3.
3. Connect to target database and copy changes from s3 to target (redshift). 

------
### How to install and run:
  1. Pull the docker image from:
```
https://github.com/puckel/docker-airflow
```
2. run:
```bash
$cd docker-airflow
$docker build --no-cache -t [CHOOSE-A-NAME] .
```
3. Open file: docker-compose-LocalExecutor.yml, under 'webserver', edit 'image' to point at the name you chose in the above step.
3. Copy the 'Dockerfile' in this repo and use it to replace the one in the puckel/airflow container.
4. run:
```
$docker-compose -f docker-compose-LocalExecutor.yml up -d
```
4. run:
```
$docker ps
```
3. Copy container ID and run:
```
$docker exec -it [CONTAINER-ID] /bin/bash
```
4. run:
```
$aws configure
```
5. Enter your credentials
6. Edit ~/.aws/config with role_arn if necessary
5. Edit ~/dags/con_defs.py, add your connection string information. 

6. Add files from this repo into the maped volum defined in the Docker file. syncdag.py defines the airflow dag. 

You're off! (hopefully)

------
### To stop:
* run:
```
$docker-compose -f docker-compose-LocalExecutor.yml down
```

------
### Classes used:
1. mssqlmachine.py - This is an object for interacting with sql database.
2. redshiftmachine.py - This is an object for interacting with redshift. 
3. s3machine.py - This object takes a db object as a parameter and uses it to interact with s3 and the given database object. 
------

### Supporting modules/files:
  1. sync_dag.py - This is the file airflow calls to get it's tasks and create a dag. 
1. con_defs.py - This file holds dictionaries with connection string info to be used by a database machine. 
2. contxfunctions.py - This file holds a contextmanager to ensure that connections are in fact closed.
3. DockerFile - This is a modified version of the Dockerfile from the puckel/airflow image. Replacing this in the original Dockerfile will generate the correct container.
------

### Pseudo code:

###### Task 1:
    Check connection definitions in con_defs.py
    Check for unfinished work from tasks that errored out. 
    Check and, if necessary, initialize source and target databases + s3 bucket for change tracking

###### Task 2:
    for each table in source database:
        if table has changes:
            write changes to s3:              
            if changes were successfully written:
                delete change history
            else:
                raise error, exit

###### Task 3:
    for each table in source database:
        if s3 has a 'changes' file for that table:
            copy from s3 to target staging table
            if copy to target staging table was successful:
                delete change file in s3
                delsert those changes from staging tabel to production table
                if delsert was successful:
                    truncate staging table
                else:
                    raise error, exit

------
### Methods and their default parameters:
##### S3 machine
```python
class s3machine:
    def __init__(self, src_dbmachine, target_dbmachine):
    def startup_procedures(self):
    def init_staging_tables(self):
    def init_rs_dummy_tables(self):
    def target_runner(self):
    def src_runner(self):
    def list_bucket_objects(self, bucket_name = BUCKETNAME): 
    def s3_put_object(self, data = '', table_name = None, file_name = ''):
    def init_s3_for_db(self):
    def s3_key_builder(self, database, table_name = '', file_name = ''):
    def list_s3_buckets(self):
    def find_s3_bucket(self, bucket_name = BUCKETNAME):
    def make_s3_bucket(self, bucket_name = BUCKETNAME, acl = 'private'):
```
##### Source database machine:
```python
class mssqlMachine():
#(When porting to a new database engine, these methods must be implemented!)
    ############################################
    def __init__(self, con_params):
    def startup_procedures(self):
    def table_has_changes(self, table_name):
    def nice_get_changes(self, table_name):
    def get_table_names(self):
    def get_column_names(self, tablename):
    def get_tables_with_pks(self):
    def get_table_primary_key(self, tablename):
    ############################################

    def clean_cdc_table(self, table):
    def get_min_lsn(self, table):
    def enable_CDC_on_db(self):
    def enable_CDC_on_tables(self):
    def get_tables_tracked_by_CDC(self):
    def check_CDC_on_user_tables(self):
    def lsn_bytearray_converter(self, time_byte_array):
    def get_sql_end_lsn(self):
    def mssql_change_test(self):
```
##### Target database machine:
```python 

class redshiftmachine():
    def __init__(self, redshift_params):
    def startup_procedures(self):
    def redshift_delsert(self, table, pk, column_name_list):
    def copy_s3_to_staging(self, table_name):
    def truncate_redshift_staging_table(self, table_name):
    def truncate_redshift_table(self, table_name):
    def analyze_redshift_db(self, table = None):
    def vacuum_redshift_db(self, table = None):
    def redshift_test(self, rs_params):
```

------

### Testing:
First restore sql database from s3 bucket:'ichain-rds-backups'

unit test files:

* test_s3machine.py
* test_mssqlmachine.py
* test_redshiftmachine.py

TODO:
unittest methods are stubs right now, still need to be populated
