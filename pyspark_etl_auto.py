# pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0

# Start docker thì comment đoạn này lại và cái spark Session
# Khi chạy trên docker nhớ thay đổi localhost thành địa chỉ IP của container 

# import findspark
# findspark.init()
import os
import time
import datetime
import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark import SparkConf, SparkContext
from uuid import * 
from uuid import UUID
import time_uuid 
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F
    
spark = SparkSession.builder.config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
    .config("spark.cassandra.connection.host", "172.17.0.2") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.registerTempTable('clicks')
    clicks_output = spark.sql(""" 
        SELECT 
            date(ts) as date,
            hour(ts) as hour,
            job_id,
            publisher_id,
            campaign_id,
            group_id,
            round(avg(bid),2) as bid_set, 
            sum(bid) as spend_hour, 
            count(*) as click 
        FROM clicks 
        GROUP BY date(ts), hour(ts), job_id, publisher_id, campaign_id, group_id 
    """)
    return clicks_output 
    
def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.registerTempTable('conversion')
    conversion_output = spark.sql(""" 
        SELECT 
            date(ts) as date,
            hour(ts) as hour,   
            job_id,
            publisher_id,
            campaign_id,
            group_id, 
            count(*) as conversion 
        FROM conversion 
        GROUP BY date(ts), hour(ts), job_id, publisher_id, campaign_id, group_id 
    """)
    return conversion_output 
    
def calculating_qualified(df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.registerTempTable('qualified')
    qualified_output = spark.sql(""" 
        SELECT 
            date(ts) as date,
            hour(ts) as hour,
            job_id,
            publisher_id,
            campaign_id,
            group_id, 
            count(*) as qualified 
        FROM qualified 
        GROUP BY date(ts), hour(ts), job_id, publisher_id, campaign_id, group_id 
    """)
    return qualified_output
    
def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.registerTempTable('unqualified')
    unqualified_output = spark.sql(""" 
        SELECT 
            date(ts) as date,
            hour(ts) as hour,
            job_id,
            publisher_id,
            campaign_id,
            group_id, 
            count(*) as unqualified 
        FROM unqualified 
        GROUP BY date(ts), hour(ts), job_id, publisher_id, campaign_id, group_id 
    """)
    return unqualified_output
    
def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
    join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full')
    return final_data 
    
def process_cassandra_data(df):
    print('Processing Calculation Clicks')
    clicks_output = calculating_clicks(df)
    print('Processing Calculation Conversion')
    conversion_output = calculating_conversion(df)
    print('Processing Calculation Qualified')
    qualified_output = calculating_qualified(df)
    print('Processing Calculation Unqualified')
    unqualified_output = calculating_unqualified(df)
    print('Processing Join Data')
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data
    
def retrieve_company_data(url,driver,user,password):
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    return company 
    
def import_to_mysql(output):
    final_output = output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversion','click','bid_set','spend_hour', 'Last_Updated_At')
    # final_output = final_output.withColumnRenamed('date','dates').withColumnRenamed('hour','hours').withColumnRenamed('qualified','qualified_application').\
    # withColumnRenamed('unqualified','disqualified_application').withColumnRenamed('conversions','conversion')
    final_output = final_output.withColumn('sources',lit('Cassandra'))
    final_output.write.format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://172.17.0.3:3306/Data_Warehouse") \
    .option("dbtable", "events_etl") \
    .mode("append") \
    .option("user", "root") \
    .option("password", "1") \
    .save()
    return print('Data imported successfully')

def last_updated_time(df, final_output):
    last_update_time = df.select(F.max("ts")).collect()[0][0]
    final_output = final_output.withColumn("Last_Updated_At", F.lit(last_update_time))
    return final_output

def main_task(mysql_time):
    host = '172.17.0.3'
    port = '3306'
    db_name = 'Data_Warehouse'
    user = 'root'
    password = '1'
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"
    print('------ Data Warehouse --------')
    print('The host is:' ,host)
    print('The port using is: ',port)
    print('The db using is: ',db_name)
    print('------------------------------')
    print('Retrieving data from Cassandra')
    print('------------------------------')
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="study_data_engineering").load().filter(col('ts') > mysql_time)
    print('------------------------------')
    print('Selecting data from Cassandra')
    print('------------------------------')
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df.printSchema()
    print('------------------------------')
    print('Processing Cassandra Output')
    print('------------------------------')
    cassandra_output = process_cassandra_data(df)
    print('------------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    company = retrieve_company_data(url,driver,user,password)
    join_output = cassandra_output.join(company,'job_id','left').drop(company.group_id).drop(company.campaign_id)
    print('-----------------------------')
    print('Add column Last Updated Time (CDC) near real time')
    print('-----------------------------')
    final_output = last_updated_time(df, join_output)
    print('-----------------------------')
    print('Data Final Output')
    print('-----------------------------')
    final_output.show()
    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output)
    return print('Task Finished')
    
def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'study_data_engineering').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time(url,driver,user,password):    
    sql = """(select max(Last_Updated_At) from events_etl) data"""
    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        # mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
        mysql_latest = mysql_time
    return mysql_latest 

host = '172.17.0.3'
port = '3306'
db_name = 'Data_Warehouse'
user = 'root'
password = '1'
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"

while True :
    start_time = datetime.datetime.now()
    cassandra_time = get_latest_time_cassandra()
    print('--------------------------------------------------')
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_mysql_latest_time(url,driver,user,password)
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time : 
        main_task(mysql_time)
    else :
        print("No new data found")
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(10)
