pyspark --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0

#read data from Cassandra 
data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'study_de').load()

#read data from MySQL

url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'data_engineering'
driver = "com.mysql.cj.jdbc.Driver"
user = 'root'
password = '1'
sql = '(select * from events) A'
df = spark.read.format('jdbc').options(url = url , driver = driver , dbtable = sql , user=user , password = password).load()

#write data to MySQL
df.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','test_import_data').option('user',user).option('password',password).mode('append').save()


data = data.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
data = data.filter(data.job_id.isNotNull())


#process click data 

click_data = data.filter(data.custom_track == 'clicks')
click_data.createTempView('clickdata')
spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id,round(avg(bid),2) as bid_set , sum(bid) as spend_hour , count(*) as click from click group by date(ts),
hour(ts),job_id,publisher_id,campaign_id,group_id""").show()

#process conversion data
#process qualified data
#process unqualified data 
#finalize output full join
#merge company_id vào output 
# cầm output import vào mysql 



 
