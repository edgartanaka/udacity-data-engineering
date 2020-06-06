# Author
Edgar Tanaka <edgartanaka@gmail.com>

# About
This project is part of the Udacity Data Engineering Nanodegree.
Here we are building a data pipeline on top of AWS S3 and Redshift.
S3 is used for storage of files and Redshift is a database for MPP (massive
parallel processing).
The project is broken down into 2 python files:
- `create_table.py` drops/creates the redshift tables
- `etl.py` does two things: first it loads data (in json format) from S3
to Redshift, second it extracts data from these staging tables in Redshift
and loads data in tables that can be easily queried by data analysts.

The pipeline looks like this:
```
AWS S3 (JSON files) -> AWS Redshift (2 staging tables) -> AWS Redshift (5 tables)
```    

# Schema
We are following the star schema here. 

## Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong
    * songplay_id
    * start_time
    * user_id
    * level
    * song_id
    * artist_id
    * session_id
    * location
    * user_agent

## Dimension Tables
* users - users in the app
    * user_id
    * first_name
    * last_name
    * gender
    * level
* songs - songs in music database
    * song_id
    * title
    * artist_id
    * year
    * duration
* artists - artists in music database
    * artist_id
    * name
    * location
    * lattitude
    * longitude
* time - timestamps of records in songplays broken down into specific units
    * start_time
    * hour
    * day
    * week
    * month
    * year
    * weekday

## How to run
Follow the sections below to run this project.

### Part 1: create your Redshift cluster
- Create a AWS account if you don't have yet
- Create a redshift cluster. Instructions here: https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html
- The redshift clusters takes a few minutes to be created. Wait until its status is set to "Available" 

## Part 2: create dwh.cfg
Create a file named `dwh.cfg` in the root of this project. Do not commit this file to git!
It should follow this template:
```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=5439

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'
```

Here is an example with fictious configuration:
```
[CLUSTER]
HOST=redshift-cluster-1.abcdefg.us-west-2.redshift.amazonaws.com
DB_NAME=dev
DB_USER=udacity
DB_PASSWORD=Udacity
DB_PORT=5439

[IAM_ROLE]
ARN='arn:aws:iam::123456789:role/myRedshiftRole'

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'
```

## Running the project
Set up your python environment:
```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create the redshift tables. 
Important Note: This will also drop the table if it previously existed.
```
python create_tables.py
```

Load data from S3 into redshift and populate star schema tables with data.
```
python etl.py
```

Now go to the Query Editor in AWS Console and run a few queries to verify if your 
data was loaded correctly into Redshift: https://us-west-2.console.aws.amazon.com/redshiftv2/home?region=us-west-2#query-editor



