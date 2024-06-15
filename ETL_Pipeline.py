# pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0

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
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F


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
    .option("url", "jdbc:mysql://localhost:3306/Data_Warehouse") \
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
    host = 'localhost'
    port = '3306'
    db_name = 'Data_Warehouse'
    user = 'root'
    password = '1'
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="study_data_engineering").load().filter(col('ts') > mysql_time)
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df.show()
    cassandra_output = process_cassandra_data(df)
    company = retrieve_company_data(url,driver,user,password)
    join_output = cassandra_output.join(company,'job_id','left').drop(company.group_id).drop(company.campaign_id)
    final_output = last_updated_time(df, join_output)
    final_output.show()
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
    
spark = SparkSession.builder.config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()
host = 'localhost'
port = '3306'
db_name = 'Data_Warehouse'
user = 'root'
password = '1'
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"

while True :
    cassandra_time = get_latest_time_cassandra()
    mysql_time = get_mysql_latest_time(url,driver,user,password)
    print(cassandra_time)
    print(mysql_time)
    if cassandra_time > mysql_time : 
        main_task(mysql_time)
    else :
        print("No new data found")
    time.sleep(10)

# main_task()
# print(get_latest_time_cassandra())
# print(get_mysql_latest_time(url,driver,user,password))
# print(get_latest_time_cassandra() > get_mysql_latest_time(url,driver,user,password)) 


 
