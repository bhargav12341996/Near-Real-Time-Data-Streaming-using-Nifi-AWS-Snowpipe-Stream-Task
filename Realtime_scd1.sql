create database if not exists scd_demo;
use database scd_demo;
create schema if not exists scd2;
use schema scd2;
show tables;

create or replace table customer (   
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     update_timestamp timestamp_ntz default current_timestamp());

create or replace table customer_history (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     start_time timestamp_ntz default current_timestamp(),
     end_time timestamp_ntz default current_timestamp(),
     is_current boolean
     );
     
create or replace table customer_raw (
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar);

---customer is the daily table and has only updated data:
create or replace stream customer_table_changes on table customer;

------Data Loading:
CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage
    url='s3://dwh-snowflake-course-bhargav/snowflake_nifi_stream_data/'
    credentials=(aws_key_id='',aws_secret_key='');

CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

LIST @customer_ext_stage;

CREATE OR REPLACE PIPE customer_s3_pipe
  auto_ingest = true
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = CSV
  ;

Show pipes;

SELECT * FROM customer_table_changes;


---Merging (Upsert):

merge into customer c 
using customer_raw cr
   on  c.customer_id = cr.customer_id
when matched and c.customer_id <> cr.customer_id or
                 c.first_name  <> cr.first_name  or
                 c.last_name   <> cr.last_name   or
                 c.email       <> cr.email       or
                 c.street      <> cr.street      or
                 c.city        <> cr.city        or
                 c.state       <> cr.state       or
                 c.country     <> cr.country then update
    set c.customer_id = cr.customer_id
        ,c.first_name  = cr.first_name 
        ,c.last_name   = cr.last_name  
        ,c.email       = cr.email      
        ,c.street      = cr.street     
        ,c.city        = cr.city       
        ,c.state       = cr.state      
        ,c.country     = cr.country  
        ,update_timestamp = current_timestamp()
when not matched then insert
           (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
    values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);

TRUNCATE customer_raw;
TRUNCATE customer;

---Creating Stored procedure:

CREATE OR REPLACE PROCEDURE pdr_scd_demo()
returns string not null
language javascript
as
    $$
      var cmd = `
                 merge into customer c 
                 using customer_raw cr
                    on  c.customer_id = cr.customer_id
                 when matched and c.customer_id <> cr.customer_id or
                                  c.first_name  <> cr.first_name  or
                                  c.last_name   <> cr.last_name   or
                                  c.email       <> cr.email       or
                                  c.street      <> cr.street      or
                                  c.city        <> cr.city        or
                                  c.state       <> cr.state       or
                                  c.country     <> cr.country then update
                     set c.customer_id = cr.customer_id
                         ,c.first_name  = cr.first_name 
                         ,c.last_name   = cr.last_name  
                         ,c.email       = cr.email      
                         ,c.street      = cr.street     
                         ,c.city        = cr.city       
                         ,c.state       = cr.state      
                         ,c.country     = cr.country  
                         ,update_timestamp = current_timestamp()
                 when not matched then insert
                            (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
                     values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
      `
      var cmd1 = "truncate table SCD_DEMO.SCD2.customer_raw;"
      var sql = snowflake.createStatement({sqlText: cmd});
      var sql1 = snowflake.createStatement({sqlText: cmd1});
      var result = sql.execute();
      var result1 = sql1.execute();
    return cmd+'\n'+cmd1;
    $$;

call pdr_scd_demo();

SELECT * from customer;
SELECT * from customer_raw;

SELECT * FROM customer_table_changes;

------SNOWTASK:

--Set up TASKADMIN role
use role securityadmin;
create or replace role taskadmin;

-- Set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to TASKADMIN:
use role accountadmin;
grant execute task on account to role taskadmin;

-- creating task:
create or replace task tsk_scd_raw warehouse = COMPUTE_WH schedule = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
call pdr_scd_demo();

show tasks;

alter task tsk_scd_raw suspend;

select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state 
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;