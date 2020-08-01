
# Movie Analytics Pipeline
Author: Edgar Tanaka

## TL;DR
This is the capstone project of the Udacity Data Engineering Nanodegree. 

I have built a data pipeline that collects movie data from disparate datasets and creates a Movie Analytics
dataset for adhoc insights.

## Step 1: Scope the Project and Gather Data
IMDB seems to be already established as a source of truth database for movies metadata. 
Even so, this catalog has been enriched with data such as tagging, genre, rating, awards, revenue and production companies. 
This enrichment expands the insights that could be extracted by a data analyst. 

Adult movies and TV series are considered out of the scope of this project.

### Use cases
Here I have outlined some questions that could be answered by querying the Movie Analytics dataset:
- What genres are the most common among the top 1000 movies with the highest profit?
- Is there a trend in most popular genres?
- Do we have more LGBT related movies now than in the past? How are they rated?
- What genres have won the most Oscars in history?
- What genres are the best rated?

## Step 2: Explore and Assess the Data

### The Datasets
- [IMDB](https://www.imdb.com/interfaces/)
- [MovieLens](https://grouplens.org/datasets/movielens/)
- [TMDB](https://developers.themoviedb.org/3)
- [The Oscars (Kaggle)](https://www.kaggle.com/unanimad/the-oscar-award)
- [Golden Globe Awards (Kaggle)](https://www.kaggle.com/unanimad/golden-globe-awards)
- [Bafta Awards (Kaggle)](https://www.kaggle.com/unanimad/bafta-awards)
- [SAGA - Screen Actors Guild Awards (Kaggle)](https://www.kaggle.com/unanimad/screen-actors-guild-awards)

Data can easily be dowloaded for IMDB, MovieLens and the Kaggle datasets. 
Note that the Kaggle datasets are not official. They were scraped by someone and published on the Kaggle
platform.

The TMDB though was trickier to get as it's offered as an API. I had to write a [python script](https://gist.github.com/edgartanaka/a1bc5a0d7bb843f19b62669cd9bb3f8e)
to download all of the movies data. I have shared this [data on Kaggle](https://www.kaggle.com/edgartanaka1/tmdb-movies-and-series)
so that anyone interested in movies data can make use of it. Each movie was downloaded from the API and saved as a
JSON file. More than 500K files are within this generated dataset. 

### Datasets in size and format
| Dataset            | Table            | Rows count | Format |
|--------------------|------------------|------------|--------|
| IMDB               | name.basics      | 10,218,303 | TSV    |
| IMDB               | title.basics     | 6,974,101  | TSV    |
| IMDB               | title.crew       | 6,974,101  | TSV    |
| IMDB               | title.principals | 40,116,933 | TSV    |
| IMDB               | title.ratings    | 1,057,712  | TSV    |
| MovieLens (ml-25m) | genome-scores    | 15,584,449 | CSV    |
| MovieLens (ml-25m) | genome-tags      | 1,129      | CSV    |
| MovieLens (ml-25m) | links            | 62,424     | CSV    |
| MovieLens (ml-25m) | movies           | 62,424     | CSV    |
| MovieLens (ml-25m) | ratings          | 25,000,096 | CSV    |
| MovieLens (ml-25m) | tags             | 1,093,361  | CSV    |
| TMDB               | movies           | 526,563    | JSON   |
| Oscars             | oscars           | 10,396     | CSV   |
| Golden Globe       | golden globe     | 7,992      | CSV   |
| BAFTA              | bafta            | 4,177      | CSV   |
| SAGA               | saga             | 5,760      | CSV   |

## TMDB
```
{
	"adult": false,
	"backdrop_path": "/ck75O6pTZPNrV51CHwd3IxOfzkI.jpg",
	"belongs_to_collection": null,
	"budget": 0,
	"genres": [{
		"id": 80,
		"name": "Crime"
	}, {
		"id": 18,
		"name": "Drama"
	}],
	"homepage": "http://www.derfreiewille.com/",
	"id": 9999,
	"imdb_id": "tt0499101",
	"original_language": "de",
	"original_title": "Der freie Wille",
	"overview": "After nine years in psychiatric detention Theo, who has brutally assaulted and raped three women, is released. Living in a supervised community, he connects well with his social worker Sascha, finds a job at a print shop and even a girlfriend, Nettie, his principal's brittle and estranged daughter. But even though superficially everything seems to work out Theo's seething rage remains ready to erupt.",
	"popularity": 4.793,
	"poster_path": "/tHaHcLZSPiIYHyTfUWQtEUAKdCs.jpg",
	"production_companies": [{
		"id": 1084,
		"logo_path": null,
		"name": "Colonia Media",
		"origin_country": ""
	}],
	"production_countries": [{
		"iso_3166_1": "DE",
		"name": "Germany"
	}],
	"release_date": "2006-08-23",
	"revenue": 0,
	"runtime": 163,
	"spoken_languages": [{
		"iso_639_1": "de",
		"name": "Deutsch"
	}, {
		"iso_639_1": "fr",
		"name": "Français"
	}],
	"status": "Released",
	"tagline": "",
	"title": "The Free Will",
	"video": false,
	"vote_average": 6.7,
	"vote_count": 24
}
```

## Step 3: Define the Data Model
**TODO** Add data dictionary of final dataset

### Overall
- snake case for fields
- no creation of internal ID
- IMDB IDs are used as IDs (as it seems to be standard in the industry) 
- IMDB database is the source of truth. If any consolidation was needed, IMDB always had higher priority.
- If data from other datasets for enrichment lacked tconst, it was discarded.
- flat tables 

![data model](img/model.png)

- Movie
    - imdb_title_id
    - original_title
    - overview
    - release_date
    - start_year
    - end_year
    - imdb_rating
    - tmdb_rating
    - movielens_rating
    - original_language
    - budget
    - revenue
    - status
```
select 
	tconst,
    titleType as movie_type,
    primaryTitle as primary_title,
    originalTitle as original_title,
    startYear as start_year,
    endYear as end_year,
    runtimeMinutes as runtime_minutes    
from imdb.title_basics
where  
isadult=0
and titleType in ('movie', 'short', 'tvmovie')
limit 100
```
    
- Genre (movielens, imdb, tmdb)
    - imdb_tconst
    - genre    
```

```

- Tag (movielens)
    - imdb_tconst
    - tag
    - relevance
```

```

- MovieAward
    - imdb_tconst
    - event
    - year
    - award_title

- PersonAward
    - imdb_nconst
    - event
    - year
    - award_title

- Person
    - imdb_const (string) - alphanumeric unique identifier of the name/person
    - primaryName (string)– name by which the person is most often credited
    - birthYear – in YYYY format
    - deathYear – in YYYY format if applicable, else '\N'
    - primaryProfession (array of strings)– the top-3 professions of the person
    - knownForTitles (array of tconsts) – titles the person is known for

- Country
    - code

- Language
    - code


## Step 4: Run ETL to Model the Data

![DAG Airflow](img/dag_capstone.png)

### Steps
Staging steps
- create_bq_datasets: drops and recreates the final schema for analytics

- ml_stage.py: stages movielens data into redshift
- imdb_stage.py: stages all the data from IMDB into redshift
- tmdb_stage.py: stage TMDB data into redshift

- movie.py: loads data into `analytics.movie`
- genre.py: load table `analytics.genre` with data from IMDB, TMDB and movielens
- tag.py: loads data into `analytics.tag`

Consolidation steps
- consolidate genres: imdb, movielens, tmdb
- consolidate movielens + imdb

Validation steps

## Movie
Filtering/Cleaning
- removed adult movies
- removed anything but movies (only kept title where type was "movie", "tvMovie" or "short")

## Genre
Filtering/Cleaning
- removed genre != '(no genres listed)'

## Thinking about other scenarios
**TODO**

Include a description of how you would approach the problem differently under the following scenarios:
If the data was increased by 100x.
If the pipelines were run on a daily basis by 7am.
If the database needed to be accessed by 100+ people. 

## Lessons learned
- redshift has bad support for flattening
- redshift has bad support for json (very limited)

Time to load TMDB json data into BQ:
```
$time python stage_json.py 
/Users/edgart/.pyenv/versions/3.6.10/lib/python3.6/site-packages/pandas/compat/__init__.py:117: UserWarning: Could not import the lzma module. Your installed Python is incomplete. Attempting to use lzma compression will result in a RuntimeError.
  warnings.warn(msg)
Starting job 1b5af311-c363-4098-9a6a-c32041ba4dd5
Job finished.
Loaded 526631 rows.

real	4m7.194s
user	0m1.424s
sys	0m0.278s
```