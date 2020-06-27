# Sparkify Data Pipepline with apache-airflow

# Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

I have been contacted to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.


## Requirements 
*  Python3
*  Apache airflow
*  AWS credentials 
   > Access_key_id = ***********
   >
   > Acess_secret_key = ********
> NOTE!! Dont make this public, always hash your key and secret key when upoading to a public repository
* Redshift config
    * HOST/ENDPOINT
    * USERNAME
    * PASSWORD
    * DB NAME
    * PORT NO.
    
    
## Source Data
The datasets are extracted from an s3 buckekts. Here are the links for each of them 
* Song data : `s3://udacity-dend/song_data`
* Log data : `s3://udacity-dend/log_data`

Both datasets have json extentions


## Data Model/Schema 
The table schema adopted here is the start table with Facts and dimension table as follows, 

### Dimension Tables
1. `users` - users in the app (resides in log database)
    * user_id, first_name, last_name, gender, level
    
2. `songs` - songs in music database (resides in song database)
    * songs_id, title, artist_id, year, duration
    
3. `artists` - artist in music database(resides in song database)
    * artist_id, name, location, latitude, longitude
    
4. `time` - timestamps of records in songplays broken down in units (resides in log database)
    * start_time , hour, day, week, month, year, weekday
    
### Fact Table    
1. `songplays` - records in log data assosicated with songs plays i.e records with page `Next Song`
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
<img src="imgs/star_schema_photo.jpg" alt="drawing" width="400"/>

## Tempalate
* `udac_example_dag.py` contains the tasks and dependencies of the DAG. Located in the `dags` directory.
* `create_tables.sql` contains the SQL queries used to create all the required tables in Redshift. ILocated in the `dag` directory.
* `sql_queries.py` contains the SQL queries used in the ETL process. Located in the `plugins/helper` directory
The following operators are be placed in the plugins/operators directory of your Airflow installation:

`stage_redshift.py` contains `StageToRedshiftOperator` :  copies JSON data from S3 to staging tables in the Redshift data warehouse.
`load_dimension.py` contains `LoadDimensionOperator` :  loads a dimension table from data in the staging table(s).
`load_fact.py` contains `LoadFactOperator`: loads a fact table from data in the staging table(s).
`data_quality.py` contains DataQualityOperator : which runs a data quality check

### Successful dag
After correctly placing the task dependencies with no errors from custom operators, sql queries or task, your result should look like this

<img src="imgs/dag.png" alt="drawing" width="400"/>
