# Author
Edgar Tanaka

# Pre-requirements
This project was implemented and tested on:
- python 3.6.10
- pyspark 3.0.0 
- conda 4.8.3

You can find instructions on how to install pyspark in your computer here: https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421

# Summary
This is the 4th project of the Udacity Data Engineering Nanodegree.
In this project, I built an ETL that reads data from S3 and writes data back to S3.
The input and output datasets are explained in more details in the following sections.
The final result of this ETL are 5 tables in a star schema format.

Using the song and log datasets, you'll need to create a star schema optimized for queries
 on song play analysis. This includes the following tables.

# Inputs
There are 2 input datasets

## Input 1: Songs dataset
This dataset is a collection of JSON files where each JSON file represents a song and its associated metadata.
Here is an example of a song JSON:
```
{
	"num_songs": 1,
	"artist_id": "ARJIE2Y1187B994AB7",
	"artist_latitude": null,
	"artist_longitude": null,
	"artist_location": "",
	"artist_name": "Line Renaud",
	"song_id": "SOUPIRU12A6D4FA1E1",
	"title": "Der Kleine Dompfaff",
	"duration": 152.92036,
	"year": 0
}
```

## Input 2: Logs datasets
The logs dataset contains instrumentation data of a music streaming app (called Sparkify).
Here is an example of an event logged:
```
{
	"artist": null,
	"auth": "Logged In",
	"firstName": "Walter",
	"gender": "M",
	"itemInSession": 0,
	"lastName": "Frye",
	"length": null,
	"level": "free",
	"location": "San Francisco-Oakland-Hayward, CA",
	"method": "GET",
	"page": "Home",
	"registration": 1540919166796.0,
	"sessionId": 38,
	"song": null,
	"status": 200,
	"ts": 1541105830796,
	"userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
	"userId": "39"
}
```

Here is an explanation of the most relevant columns:

| Column   | Description                                                                                       |
|----------|---------------------------------------------------------------------------------------------------|
| artist   | name of the artist                                                                                |
| song     | name of the song                                                                                  |
| page     | page visited or action. Songplays are defined only by the records where page equals to 'NextSong' |
| location | user's location                                                                                   |
| ts       | epoch time in milliseconds                                                                        |


# Outputs
This ETL generates 5 tables in a star schema format. 
The fact table is "songplays" and contains all the events of a song played in the music streaming app.
The dimension tables contains more detailed information about the user of the app,
the song played, the artist who performed the song and the date/time when the song play event occured.

## Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong
    * songplay_id
    * start_time: time in epoch (milliseconds)
    * user_id: ID of the user
    * level: type of account. It can be either "free" or "paid"
    * song_id: ID of the sond
    * artist_id: ID of the artist
    * session_id: ID of the user session
    * location: location of the user
    * user_agent: user agent shares information about the platform and client used by the user

## Dimension Tables
* users - users in the app
    * user_id: ID of the user
    * first_name: first name of the user
    * last_name: last name of the user
    * gender: gender. Can be either "M" or "F"
    * level: type of account. It can be either "free" or "paid"
    
* songs - songs in music database
    * song_id: ID of the song
    * title: title of the song
    * artist_id: ID of the artist who performed the song
    * year: year of the recording
    * duration: duration of the song performance
    
* artists - artists in music database
    * artist_id: ID of the artist 
    * name: name of the artist
    * location: city and state where the artist works
    * latitude: coordinates where the artist works 
    * longitude: coordinates where the artist works 
    
* time - timestamps of records in songplays broken down into specific units
    * start_time: time in epoch (milliseconds)
    * hour
    * day
    * week
    * month
    * year
    * weekday

# Files in this repository
- etl.py: reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfg: contains your AWS credentials
- README.md: provides discussion on your process and decisions

 # How to run 
First, name sure you have all the pre-requirements installed and configured.

Then, unzip the data files: 
```
cd data
unzip log-data.zip
unzip song-data.zip
```

Then install the python libs and run it as any other python script.
```
conda create --name udacity
source activate udacity
pip install -r requirements.txt
python etl.py
```

The output datasets will be created in the data directory.

