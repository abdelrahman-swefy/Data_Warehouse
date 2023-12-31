# Data Warehouse ETL Pipeline

## Introduction

This project aims to build an ETL (Extract, Transform, Load) pipeline for a music streaming startup called Sparkify. The goal is to move their user and song data onto the cloud by extracting the data from S3 (Amazon Simple Storage Service), staging it in Redshift (Amazon Redshift), and transforming it into dimensional tables for analytics purposes. The ETL pipeline will allow Sparkify's analytics team to gain insights into the songs their users are listening to.

## System Architecture

The system architecture for the AWS S3 to Redshift ETL pipeline involves the following steps:

Data extraction: The data is extracted from the S3 buckets that contain two types of JSON datasets:
- Song data: Contains metadata about songs and artists.
- Log data: Contains user activity logs generated by the music streaming app.

Data staging: The extracted data is staged in Redshift using staging tables. Staging tables act as temporary storage for the raw data before it is transformed into the final analytics tables.

Data transformation: SQL statements are executed to transform the data from the staging tables into a set of dimensional tables. The dimensional tables are optimized for efficient querying and analysis.

### Dimensional tables:

##### Fact table: 
"songplays" table that records the events associated with song plays, such as song play ID, start time, user ID, song ID, artist ID, etc.  

##### Dimension tables:
"users" table: Contains information about the app users, including user ID, first name, last name, gender, and user level.  
"songs" table: Stores information about the songs in the music database, including song ID, title, artist ID, year, and duration.  
"artists" table: Contains details about the artists in the music database, including artist ID, name, location, latitude, and longitude.  
"time" table: Stores timestamps of song plays broken down into specific units such as hour, day, week, month, year, and weekday.  


### Project Datasets
The project utilizes the following datasets stored in S3:

Song data: Located at s3://udacity-dend/song_data  
Log data: Located at s3://udacity-dend/log_data  

## Project Instructions

To complete the project, follow these steps:

1- Create staging_songs and staging_events tables on RedShift Cluster  
2- Load the data from Se into the staging tables on Redshift  
3- Create fact table ( songplays )  
4- Create dimensional tables ( users, songs, artists, time)  
5- load data from staging tables into fact and dimensional tables  

## Explanation of Files in the Repository

The repository contains the following files:

sql_queries.py: Contains SQL statements for table creation, data insertion, and data transformation.
create_table.py: This script contains Two methods. The first one is used to drop tables on redshift, the second one is used to create tables. those functions use the sql statments imported from sql_queries.py
etl.py: This script extracts data from the S3 buckets, stages it in Redshift, and transforms it into the dimensional tables using sql statments imported from sql_queries.py
dwh.cfg: Configuration file that stores AWS and Redshift connection details, file paths, and other necessary parameters.


How to Run the Python Scripts

To run the Python scripts provided in this project, follow the steps below:

1- Run create_table.py to drop existing table and create new ones  
2- Run etl.py to extracts data from the S3 buckets, stages it in Redshift, and transforms it into the dimensional tables.

## Conclusion
The Data Warehouse ETL Pipeline project enables Sparkify to efficiently extract, transform, and load their user and song data into Redshift, creating a set of dimensional tables for analysis. By following the project instructions and utilizing the provided template files, you can successfully build the ETL pipeline and empower the analytics team to derive valuable insights from the data.
