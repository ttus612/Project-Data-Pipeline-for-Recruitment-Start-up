#!/bin/sh
#source /.env
echo "DB_HOST=$DB_HOST"
echo "DB_PORT=$DB_PORT"
echo "DB_NAME=$DB_NAME"
echo "MQTT_HOST=$MQTT_HOST"
echo "MQTT_PORT=$MQTT_PORT"
echo "CASSANDRA_HOST=$CASSANDRA_HOST"
mkdir /tmp/.ivy

exec /opt/spark/bin/spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.cores=1 \
    --conf spark.ui.retainedJobs=200 \
    --conf spark.sql.shuffle.partitions=50 \
    --conf spark.jars.ivy=/tmp/.ivy \
    /opt/application/pyspark_etl_auto.py