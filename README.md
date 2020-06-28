# Author
Edgar Tanaka

# About
This is Project 5 where we exercise a data pipeline in Apache Airflow

# Project Overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

# How to run
- install airflow: https://airflow.apache.org/docs/stable/start.html
- create a redshift cluster
- create 2 connections in airflow:
    - aws_credentials (type Amazone Web Services)
    - redshift (type postgres)
- create tables in redshift with the script `create_tables.sql`
- run DAG    

