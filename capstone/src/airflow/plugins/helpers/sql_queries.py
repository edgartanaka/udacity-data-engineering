class SqlQueries:
    analytics_movie_insert = ("""
        SELECT
          imdb.tconst,
          imdb.titleType as movie_type,
          imdb.primaryTitle AS primary_title,
          imdb.originalTitle AS original_title,
          imdb.startYear AS start_year,
          imdb.endYear AS end_year,
          imdb.runtimeMinutes AS runtime_minutes,
          tmdb.budget,
          tmdb.homepage,
          tmdb.original_language,
          tmdb.overview,
          tmdb.popularity,
          tmdb.poster_path,
          tmdb.production_companies,
          tmdb.production_countries,
          tmdb.release_date,
          tmdb.revenue,
          tmdb.runtime,
          tmdb.spoken_languages,
          tmdb.status,
          tmdb.tagline,
          tmdb.vote_average,
          tmdb.vote_count
        FROM
          `edgart-experiments.imdb.title_basics` imdb
        LEFT JOIN
          `edgart-experiments.tmdb.movies` tmdb
        ON
          tmdb.imdb_id = imdb.tconst
        WHERE
          isAdult = FALSE
          AND titleType IN ('short',
            'movie',
            'tvMovie')
    """)

    analytics_person_insert = ("""
        SELECT 
          nconst, 
          primaryName as primary_name,
          CAST(birthYear AS INT64) as birth_year,
          CAST(deathYear AS INT64) as death_year,
          primaryProfession as primary_profession,
          knownForTitles as known_for_titles
        FROM `edgart-experiments.imdb.name_basics`
    """)

    analytics_movie_person_insert = ("""
        SELECT  
          tp.tconst,
          tp.nconst,
          m.primary_title as movie_primary_title,
          p.primary_name as person_primary_name,
          tp.ordering,
          tp.category,
          tp.job,
          tp.characters
        FROM `edgart-experiments.imdb.title_principals` tp
        JOIN `edgart-experiments.analytics.movie` m ON m.tconst=tp.tconst
        JOIN `edgart-experiments.analytics.person` p ON p.nconst=tp.nconst    
    """)

    analytics_genre_insert = ("""
        WITH imdb_genres AS (
            SELECT 
              tconst, -- string
              genre
            FROM `edgart-experiments.imdb.title_basics`,
            UNNEST(split(lower(genres), ',')) as genre
        ),
        
        ml_genres AS (
            SELECT 
            l.imdbId as tconst, -- imdbId is an integer
            genre
            FROM `edgart-experiments.ml.movies` m,
            UNNEST(split(lower(m.genres), '|')) as genre
            JOIN `edgart-experiments.ml.links` l on l.movieId=m.movieId
            WHERE genre != '(no genres listed)'
        ),
        
        ml_genres_with_tconst AS (
            SELECT m.tconst, ml.genre
            FROM ml_genres ml 
            JOIN `edgart-experiments.analytics.movie` m -- join only movies (ignore series and others)
            ON CAST(REPLACE(m.tconst, 'tt', '') AS INT64)=ml.tconst -- join based on integer
        ),
        all_genres_with_duplicates AS (
            SELECT * FROM ml_genres_with_tconst
            UNION ALL
            SELECT * FROM imdb_genres
        ),
        all_genres_without_duplicates AS (
            SELECT DISTINCT tconst, genre 
            FROM all_genres_with_duplicates
        )
        
        SELECT tconst, genre
        FROM all_genres_without_duplicates    
    """)

    analytics_rating_insert = ("""
        WITH ml_rating AS (
          SELECT
            m.tconst,
            TRUNC(AVG(rating), 1) as rating,
            COUNT(1) as num_votes
          FROM `edgart-experiments.ml.ratings` r
          JOIN `edgart-experiments.ml.links` l ON l.movieId=r.movieId 
          JOIN `edgart-experiments.analytics.movie` m ON CAST(REPLACE(m.tconst, 'tt', '') AS INT64)=l.imdbId
          GROUP BY m.tconst
        )
        SELECT
        m.tconst,
        imdb.averageRating as imdb_rating,
        imdb.numVotes as imdb_num_votes,
        tmdb.vote_average as tmdb_rating,
        tmdb.vote_count as tmdb_num_votes,
        ml.rating as ml_rating,
        ml.num_votes as ml_num_votes
        FROM `edgart-experiments.analytics.movie` m
        LEFT JOIN `edgart-experiments.imdb.title_ratings` imdb ON imdb.nconst=m.tconst
        LEFT JOIN `edgart-experiments.tmdb.movies` tmdb ON tmdb.imdb_id=m.tconst
        LEFT JOIN ml_rating ml ON ml.tconst=m.tconst
    """)

    analytics_tag_insert = ("""
        SELECT  
          m.tconst,
          gt.tag,
          gs.relevance
        FROM `edgart-experiments.ml.genome_scores` gs
        JOIN `edgart-experiments.ml.genome_tags` gt ON gt.tagId=gs.tagId
        JOIN `edgart-experiments.ml.links` l ON l.movieId=gs.movieId
        JOIN `edgart-experiments.analytics.movie` m ON CAST(REPLACE(m.tconst, 'tt', '') AS INT64)=l.imdbId
    """)

    analytics_production_company_insert = ("""
        SELECT 
          tmdb.imdb_id as tconst, 
          pc.name as production_company_name, 
          pc.origin_country as production_company_country
        FROM `edgart-experiments.tmdb.movies` tmdb,
        UNNEST(production_companies) as pc
        JOIN `edgart-experiments.analytics.movie` m ON tmdb.imdb_id=m.tconst
        WHERE imdb_id is not null and imdb_id != ''    
    """)

    analytics_award_oscar_insert = ("""
        SELECT  
          'SAG Awards' as award_name,
          SUBSTR(saga.year, 0, 4) as award_year,
          saga.category as award_category,
          saga.won as award_winner,  
          saga.show as film,
          saga.full_name as person_name,  
          m.tconst as tconst, 
          p.nconst as nconst
        FROM `edgart-experiments.awards.saga` saga
        JOIN `edgart-experiments.analytics.person` p ON LOWER(p.primary_name)=LOWER(saga.full_name)
        JOIN `edgart-experiments.analytics.movie` m ON LOWER(saga.show)=LOWER(m.primary_title)
        JOIN `edgart-experiments.analytics.movie_person` mp ON mp.nconst=p.nconst AND m.tconst=mp.tconst
        WHERE 
          LOWER(saga.category) NOT LIKE '%television%' AND LOWER(saga.category) NOT LIKE '%series%' -- remove TV series
          AND saga.year IS NOT NULL    
    """)


    analytics_award_golden_globe_insert = ("""
    WITH golden_globe_person AS (
      SELECT  
      gg.year_award as award_year,
      gg.category as award_category,
      gg.win as award_winner,  
      gg.film as film,
      gg.nominee as person_name,  
      m.tconst as tconst, 
      p.nconst as nconst
      FROM `edgart-experiments.awards.golden_globe` gg
      JOIN `edgart-experiments.analytics.person` p ON lower(p.primary_name)=lower(gg.nominee)
      JOIN `edgart-experiments.analytics.movie` m ON lower(gg.film)=lower(m.primary_title) AND gg.year_film=CAST(start_year AS INT64)
      JOIN `edgart-experiments.analytics.movie_person` mp ON mp.nconst=p.nconst AND m.tconst=mp.tconst
      WHERE gg.category NOT LIKE '%Television%' AND gg.category NOT LIKE '%Series%' -- remove TV series
    ),
    golden_globe_movie AS (
      SELECT  
      gg.year_award as award_year,
      gg.category as award_category,
      gg.win as award_winner,  
      gg.film as film,
      gg.nominee as person_name,  
      m.tconst as tconst, 
      p.nconst as nconst
      FROM `edgart-experiments.awards.golden_globe` gg
      JOIN `edgart-experiments.analytics.person` p ON lower(p.primary_name)=lower(gg.nominee)
      JOIN `edgart-experiments.analytics.movie` m ON lower(gg.film)=lower(m.primary_title) AND gg.year_film=CAST(start_year AS INT64)
      WHERE gg.category NOT LIKE '%Television%' AND gg.category NOT LIKE '%Series%' -- remove TV series
    ),
    all_golden_globe AS (
      SELECT * FROM golden_globe_movie
      UNION ALL
      SELECT * FROM golden_globe_person
      )
      
    -- removes duplicates while keeping non null values
    SELECT 
      'golden_globe' as award_name,
      m.award_year,
      m.award_category,
      m.award_winner,
      m.film,
      m.person_name,
      MAX(m.tconst) as tconst, -- keeps non null value
      MAX(m.nconst) as nconst -- keeps non null value
      FROM all_golden_globe m
      GROUP BY 1,2,3,4,5,6
    """)

    analytics_award_saga_insert = ("""
        SELECT  
          'SAG Awards' as award_name,
          SUBSTR(saga.year, 0, 4) as award_year,
          saga.category as award_category,
          saga.won as award_winner,  
          saga.show as film,
          saga.full_name as person_name,  
          m.tconst as tconst, 
          p.nconst as nconst
        FROM `edgart-experiments.awards.saga` saga
        JOIN `edgart-experiments.analytics.person` p ON LOWER(p.primary_name)=LOWER(saga.full_name)
        JOIN `edgart-experiments.analytics.movie` m ON LOWER(saga.show)=LOWER(m.primary_title)
        JOIN `edgart-experiments.analytics.movie_person` mp ON mp.nconst=p.nconst AND m.tconst=mp.tconst
        WHERE 
          LOWER(saga.category) NOT LIKE '%television%' AND LOWER(saga.category) NOT LIKE '%series%' -- remove TV series
          AND saga.year IS NOT NULL
    """)