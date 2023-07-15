"""This script answers the ../README.md questions.
It has 3 different parts:
A. QUERIES: definition of queries
B. QUERIES EXECUTION: queries execution
C. REPORT GENERATION: generating the report
"""

import sqlite3
from time import time
import matplotlib.pyplot as plt
import pandas as pd
import datetime
from functions import get_rating_per_gender, get_hist, convert_to_unix_time


# A. QUERIES

# 1.1 
# get total number of films
nb_films_query = "SELECT COUNT(DISTINCT movie_id) FROM movies WHERE movie_id NOT NULL"

# 1.2
# get total number of users
nb_users_query = "SELECT COUNT(DISTINCT user_id) FROM ratings WHERE user_id NOT NULL"

# 1.3 
# distribution des notes renseignées
nb_ratings_per_movie_query = "SELECT rating FROM ratings WHERE rating NOT NULL"

# 1.4 
# frequency table of ratings
rating_freq_tab_query = """
SELECT 
    ROUND((COUNT(CASE WHEN rating = 0 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_0,
    ROUND((COUNT(CASE WHEN rating = 1 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_1,
    ROUND((COUNT(CASE WHEN rating = 2 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_2,
    ROUND((COUNT(CASE WHEN rating = 3 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_3,
    ROUND((COUNT(CASE WHEN rating = 4 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_4,
    ROUND((COUNT(CASE WHEN rating = 5 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_5,
    ROUND((COUNT(CASE WHEN rating = 6 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_6,
    ROUND((COUNT(CASE WHEN rating = 7 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_7,
    ROUND((COUNT(CASE WHEN rating = 8 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_8,
    ROUND((COUNT(CASE WHEN rating = 9 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_9,
    ROUND((COUNT(CASE WHEN rating = 10 THEN 1 ELSE NULL END) * 100.0 / COUNT(rating)), 2) AS prop_10
FROM ratings
"""

# 2.1 
# add 'liked' column query
add_empty_liked_col_query = "ALTER TABLE ratings ADD COLUMN liked INTEGER"

# fill 'liked' column query
fill_liked_col_query = """
UPDATE ratings
SET liked = CASE 
    WHEN rating BETWEEN 0 AND 6 THEN 0
    WHEN rating BETWEEN 7 AND 10 THEN 1
    END;
"""

# 2.2 
# query for top 10 rated movie genders combination
# info: gender columns are combination of genders seperated by '|'
gender_combin_with_ratings_query = """
    SELECT m.genre, r.rating
    FROM movies AS m
    INNER JOIN ratings AS r 
    ON m.movie_id = r.movie_id
    WHERE m.genre NOT NULL AND r.rating NOT NULL
"""

# 3.1
# query for top 10 movies
top10_movies_query = """
    SELECT m.title, ROUND(AVG(r.rating), 2) AS avg_rating
    FROM movies AS m
    INNER JOIN ratings AS r
    ON m.movie_id = r.movie_id
    GROUP BY m.movie_id
    HAVING COUNT(r.rating) > 5
    ORDER BY avg_rating DESC
    LIMIT 10
"""

# 3.2
# get 2020 start and end dates for 2020
time_format = "%Y:%m:%d:%H:%M:%S"

unix_2020_start = convert_to_unix_time(time_str="2020:01:01:00:00:00",
                                       format_str=time_format)
unix_2020_end = convert_to_unix_time("2020:12:31:23:59:59",
                                     format_str=time_format)

# query for the 10 best rated movies in 2020
best_rated_movie_in_2020_query = f"""
    SELECT m.title, ROUND(AVG(r.rating), 2) AS avg_rating
    FROM movies AS m
    INNER JOIN ratings AS r
    ON m.movie_id = r.movie_id
    WHERE r.rating_timestamp BETWEEN {unix_2020_start} AND {unix_2020_end}
    GROUP BY m.movie_id
    HAVING COUNT(r.rating) > 5
    ORDER BY avg_rating DESC
    LIMIT 1
"""

# 4.1
# query for user_id index creation
create_index_userid_query = "CREATE INDEX idx_user_id ON ratings(user_id);"

# queries for retrieving information with and without indexation
search_by_index_query = "SELECT * FROM ratings INDEXED BY idx_user_id WHERE user_id = 255"
search_by_value_query = "SELECT * FROM ratings WHERE user_id = 255"


# B. QUERIES EXECUTION

with sqlite3.connect("../data/movies.sqlite") as co:
    # define cursor object for db modifications
    cursor_obj = co.cursor()


    # 1.1
    # get total number of films
    nb_films = co.execute(nb_films_query).fetchall()[0][0]


    # 1.2
    # get number of users
    nb_users = co.execute(nb_users_query).fetchall()[0][0]


    # 1.3
    # distribution des notes
    rating_col = co.execute(nb_ratings_per_movie_query).fetchall()
    rating_col = [rating_value[0] for rating_value in rating_col]


    # 1.4
    # get rating frequency table
    rating_freq_tab = co.execute(rating_freq_tab_query).fetchall()
    ratings_freq = [item for item in rating_freq_tab[0]]


    # 2.1
    try:
        # add an empty 'liked' column
        cursor_obj.execute(add_empty_liked_col_query)
    except sqlite3.OperationalError:
        # Exception expected to occur: the liked column already added:
        # Nothing to worry about
        pass
    
    # fill the 'liked' column
    cursor_obj.execute(fill_liked_col_query)


    # 2.2
    # best rated movie genders 
    gender_combin_with_ratings_tab = co.execute(gender_combin_with_ratings_query).fetchall()
    # get genders ranked with the average rating score
    genders_avg_rating_df = get_rating_per_gender(gender_combin_with_ratings_tab)
    top10_genders_list = genders_avg_rating_df.loc[:, 'genre'].to_list()[:10]


    # 3.1
    # top 10 most liked movies
    top10_movies_list_with_avg_score = co.execute(top10_movies_query).fetchall()
    top10_movies_list = [item[0] for item in top10_movies_list_with_avg_score]
    
    # ratings distribution
    get_hist(x=rating_col,
            xlabel="notes",
            ylabel="reccurence",
            plot_title="Distribution des notations des films dans la database movies",
            saving_path="../figures/hist_notes.png")

    # 3.2
    # top 10 most liked movies in 2020
    best_rated_movie_in_2020 = co.execute(best_rated_movie_in_2020_query).fetchall()[0][0]
    
    # 4.1
    try:
        # try to create index for user_id in ratings table
        cursor_obj.execute(create_index_userid_query)
    except sqlite3.OperationalError:
        # exception expected to occur: idx_user_id already created
        pass

    # computation time spent for retrieving ratings of user
    # using user_id
    t0_classic_search = time()
    co.execute(search_by_value_query)
    tf_classic_search = time()
    perf_classic_search = "{:.2e}".format(tf_classic_search - t0_classic_search)
    
    # using user_id index
    t0_index_search = time()
    co.execute(search_by_index_query)
    tf_index_search = time()
    perf_index_search = "{:.2e}".format(tf_index_search - t0_index_search)


# C. REPORT GENERATION
