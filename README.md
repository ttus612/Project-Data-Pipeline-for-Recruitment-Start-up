# The system processes and analyzes log data from the online recruitment platform
## Objective
This project focuses on building a data processing and analysis system from log data from users of a recruitment website. The main goal is to store, process, analyze that log data and decide the next step of the business development.
Put the raw data to Cassandra (Data Lake), PySpark for transforming the data from Cassandra then put the processed data to MySQL, MySQL for data warehousing. Using Docker to deploy the project easily.
- Tech stack: PySpark, Docker, Cassandra, MySQL, Python.
- Tools: DataGrip, Docker Desktop, Visual Studio Code, MobaXtem, NotePad++.

## Architecture
  ![image](img/Diagram_Project_Data_Pipeline_for_Recruitment_Start_up.jpg)

### Raw Data
- Log data from the website is processed near real-time into Data Lake is Cassandra.
- Log data schema
```sh
.
root
 |-- create_time: string (nullable = false)
 |-- bid: integer (nullable = true)
 |-- bn: string (nullable = true)
 |-- campaign_id: integer (nullable = true)
 |-- cd: integer (nullable = true)
 |-- custom_track: string (nullable = true)
 |-- de: string (nullable = true)
 |-- dl: string (nullable = true)
 |-- dt: string (nullable = true)
 |-- ed: string (nullable = true)
 |-- ev: integer (nullable = true)
 |-- group_id: integer (nullable = true)
 |-- id: string (nullable = true)
 |-- job_id: integer (nullable = true)
 |-- md: string (nullable = true)
 |-- publisher_id: integer (nullable = true)
 |-- rl: string (nullable = true)
 |-- sr: string (nullable = true)
 |-- ts: string (nullable = true)
 |-- tz: integer (nullable = true)
 |-- ua: string (nullable = true)
 |-- uid: string (nullable = true)
 |-- utm_campaign: string (nullable = true)
 |-- utm_content: string (nullable = true)
 |-- utm_medium: string (nullable = true)
 |-- utm_source: string (nullable = true)
 |-- utm_term: string (nullable = true)
 |-- v: integer (nullable = true)
 |-- vp: string (nullable = true)
```
<img width="1000" alt="image" src="img/schema.PNG">

- Log several data
<img width="1000" alt="image" src="img/several_data.PNG ">


### Processing Data
Read and review the data recording user actions in the log data, notice that there are actions with analytical value in the column ```["custom_track"]``` including: ```clicks, conversion, qualified, unqualified```.
Processing raw data to obtain valuable clean data:
- Filter actions with analytical value in column ```["custom_track"]``` including: ```clicks, conversion, qualified, unqualified```.
- Remove null values, replace with 0 to be able to calculate.
- Calculate the basic values of data for in-depth analysis.
- Use pySpark to write Spark jobs and process data efficiently.
- Data after processing is saved to Data Warehouse is MySQL for storage and in-depth analysis.
- Use airflow to schedule Spark-jobs every 6 Am every day.

### Clean data
- Clean Data schema
```sh
.
root
 |-- dates: timestamp (nullable = true)
 |-- hours: integer (nullable = true)
 |-- job_id: integer (nullable = true)
 |-- publisher_id: integer (nullable = true)
 |-- campaign_id: integer (nullable = true)
 |-- group_id: integer (nullable = true)
 |-- bid_set: double (nullable = true)
 |-- spend_hour: double (nullable = true)
 |-- clicks: integer (nullable = true)
 |-- conversion: integer (nullable = true)
 |-- qualified: integer (nullable = true)
 |-- unqualified: integer (nullable = true)
 |-- company_id: integer (nullable = true)
 |-- sources: string (nullable = true)
 |-- latest_update_time: timestamp (nullable = true)
```
<img width="1000" alt="image" src="img/result_finish.png>


## Setup
### Pre-requisite
#### Spark setup
- Install Spark (used 3.5.0)
#### Cassandra setup
- Install Cassandra
#### MySQL setup
- Install MySQL

 