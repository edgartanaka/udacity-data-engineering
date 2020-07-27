
# Scope
Build a database for movies analytics.

## Out of scope
- adult movies
- TV series
 

## User Story
An independent film is trying to find it's next hit.

## Use cases
The database should allow for the following questions to be answered
- What are the most profitable genres in the last 3 years?
- Is there a trend in most popular genres?
- What countries have produced the most blockbusters?
- What countries have produced smaller movies?
- Are themes about minorities more present now? How are the ratings on these?

# Data Exploration
Notebooks

## Datasets
https://www.kaggle.com/unanimad/the-oscar-award
https://www.kaggle.com/unanimad/golden-globe-awards
https://www.kaggle.com/unanimad/bafta-awards
https://www.kaggle.com/unanimad/screen-actors-guild-awards
https://www.imdb.com/interfaces/
https://grouplens.org/datasets/movielens/
https://developers.themoviedb.org/3 (API)

## IMDB
title.akas.tsv.gz - Contains the following information for titles:
- titleId (string) - a tconst, an alphanumeric unique identifier of the title
- ordering (integer) – a number to uniquely identify rows for a given titleId
- title (string) – the localized title
- region (string) - the region for this version of the title
- language (string) - the language of the title
- types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
- attributes (array) - Additional terms to describe this alternative title, not enumerated
- isOriginalTitle (boolean) – 0: not original title; 1: original title

title.basics.tsv.gz - Contains the following information for titles:
- tconst (string) - alphanumeric unique identifier of the title
- titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
- primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
- originalTitle (string) - original title, in the original language
- isAdult (boolean) - 0: non-adult title; 1: adult title
- startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year
- endYear (YYYY) – TV Series end year. ‘\N’ for all other title types
- runtimeMinutes – primary runtime of the title, in minutes
- genres (string array) – includes up to three genres associated with the title

title.crew.tsv.gz – Contains the director and writer information for all the titles in IMDb. Fields include:
- tconst (string) - alphanumeric unique identifier of the title
- directors (array of nconsts) - director(s) of the given title
- writers (array of nconsts) – writer(s) of the given title

title.episode.tsv.gz – Contains the tv episode information. Fields include:
- tconst (string) - alphanumeric identifier of episode
- parentTconst (string) - alphanumeric identifier of the parent TV Series
- seasonNumber (integer) – season number the episode belongs to
- episodeNumber (integer) – episode number of the tconst in the TV series

title.principals.tsv.gz – Contains the principal cast/crew for titles
- tconst (string) - alphanumeric unique identifier of the title
- ordering (integer) – a number to uniquely identify rows for a given titleId
- nconst (string) - alphanumeric unique identifier of the name/person
- category (string) - the category of job that person was in
- job (string) - the specific job title if applicable, else '\N'
- characters (string) - the name of the character played if applicable, else '\N'

title.ratings.tsv.gz – Contains the IMDb rating and votes information for titles
- tconst (string) - alphanumeric unique identifier of the title
- averageRating – weighted average of all the individual user ratings
- numVotes - number of votes the title has received

name.basics.tsv.gz – Contains the following information for names:
- nconst (string) - alphanumeric unique identifier of the name/person
- primaryName (string)– name by which the person is most often credited
- birthYear – in YYYY format
- deathYear – in YYYY format if applicable, else '\N'
- primaryProfession (array of strings)– the top-3 professions of the person
- knownForTitles (array of tconsts) – titles the person is known for


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

# Data Model
## Overall
- snake case for fields
- no creation of internal ID
- IMDB IDs are used as IDs (as it seems to be standard in the industry) 
- IMDB database is the source of truth. If any consolidation was needed, IMDB always had higher priority.
- If data from other datasets for enrichment lacked tconst, it was discarded.
- flat tables 


PICTURE HERE

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


# ETL
## Steps
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


# Dataset sizes
Sizes in rows
ml-latest
```
 15584449 ml-25m/genome-scores.csv
    1129 ml-25m/genome-tags.csv
   62424 ml-25m/links.csv
   62424 ml-25m/movies.csv
 25000096 ml-25m/ratings.csv
 1093361 ml-25m/tags.csv
```

IMDB
```
 10,218,303 name.basics.tsv
  6,974,101 title.basics.tsv
  6,974,101 title.crew.tsv
 40,116,933 title.principals.tsv
```

TMDB
526563 movies


--select * from stl_load_errors order by starttime desc
--select count(1) from tmdb.movies

create external schema tmdbsp 
from data catalog 
database 'tmdbsp' 
iam_role 'arn:aws:iam::877437751008:role/RedshiftCopyUnload'
create external database if not exists;


CREATE external TABLE "tmdbsp.movies" (
    "adult" boolean,
    "backdrop_path" varchar(2000),
    "belongs_to_collection" varchar(2000),
    "budget" int,
    "genres" varchar(4000),
    "homepage" varchar(2000),
    "id" int,
    "imdb_id" varchar(32),
    "original_language" varchar(2),
    "original_title" varchar(4000),
    "overview" varchar(8000),
    "popularity" numeric(10,2),
    "poster_path" varchar(4000),
    "production_companies" varchar(4000),
    "production_countries" varchar(4000),
    "release_date" varchar(10),
    "revenue" bigint,
    "runtime" bigint,
    "spoken_languages" varchar(4000),
    "status" varchar(100),
    "tagline" varchar(4000),
    "title" varchar(4000),
    "video" boolean,
    "vote_average" numeric(3,1),
    "vote_count" int)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
with serdeproperties (
'dots.in.keys' = 'true',
'mapping.requesttime' = 'requesttimestamp'
) 
location 's3://udacity-de-tmdb/movies/movies/'

# Lessons learned
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