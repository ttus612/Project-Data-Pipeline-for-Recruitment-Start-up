FROM apache/spark-py:v3.1.3

USER root
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
RUN apt-get update
RUN apt install -y python3 python3-pip
RUN pip3 install --upgrade pip setuptools --user
RUN rm -r /root/.cache && rm -rf /var/cache/apt/*

WORKDIR /opt/application
COPY requirements.txt .
COPY pyspark_etl_auto.py /opt/application/pyspark_etl_auto.py
COPY mysql-connector-java-8.0.30.jar /opt/spark/jars
COPY entrypoint.sh /entrypoint.sh

RUN pip3 install -r requirements.txt --user

ENTRYPOINT ["sh","/entrypoint.sh"]