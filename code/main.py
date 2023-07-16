"""This script answers the ../README.md questions.
It has 3 different parts:
A. QUERIES: defining most of the needed queries
B. QUERIES EXECUTION: queries execution part
C. REPORT GENERATION: generating the report (see ../results.txt)
"""


import sqlite3
from time import time

from functions import (get_rating_per_gender, get_hist,
                       convert_to_unix_time, get_ratings_frequency_table_query)


# A. QUERIES

# 1.1 
# get total number of films
nb_films_query = "SELECT COUNT(DISTINCT movie_id) FROM movies WHERE movie_id NOT NULL"

# 1.2
# get total number of users
nb_users_query = "SELECT COUNT(DISTINCT user_id) FROM ratings WHERE user_id NOT NULL"

# 1.3 
# distribution des notes renseignées
rating_col_vec_query = "SELECT rating FROM ratings WHERE rating NOT NULL"

# 1.4 
# frequency table of ratings: built with 1.3 results

# 2.1 
# add 'liked' column query
add_empty_liked_col_query = "ALTER TABLE ratings ADD COLUMN liked INTEGER"

# fill 'liked' column query: 0 if rating [0, 6]; 1 if rating [7, 10]
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
# example: (History|Fiction|News)
gender_combin_with_ratings_query = """
    SELECT m.genre, r.rating
    FROM movies AS m
    INNER JOIN ratings AS r 
    ON m.movie_id = r.movie_id
    WHERE m.genre NOT NULL AND r.rating NOT NULL
"""

# 3.1
# query for top 10 rated movies on average in the whole dataset
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
# get start and end dates for 2020 from unix format
time_format = "%Y:%m:%d:%H:%M:%S"

unix_2020_start = convert_to_unix_time(time_str="2020:01:01:00:00:00",
                                       format_str=time_format)
unix_2020_end = convert_to_unix_time("2020:12:31:23:59:59",
                                     format_str=time_format)

# query for the best rated movie in 2020
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
# query for user_id index creation: idx_user_id
create_index_userid_query = "CREATE INDEX idx_user_id ON ratings(user_id);"

# queries for retrieving information with and without indexation
search_with_index_query = "SELECT * FROM ratings INDEXED BY idx_user_id WHERE user_id = 255"
search_without_index_query = "SELECT * FROM ratings WHERE user_id = 255"


# B. QUERIES EXECUTION

with sqlite3.connect("../data/movies.sqlite") as co:
    # define cursor object for db modifications
    cursor_obj = co.cursor()

    # 1.1 get total number of films
    nb_films = co.execute(nb_films_query).fetchall()[0][0]

    # 1.2 get total number of users
    nb_users = co.execute(nb_users_query).fetchall()[0][0]

    # 1.3 ratings distribution
    rating_col_vec = co.execute(rating_col_vec_query).fetchall()
    ratings_vec = [rating_value[0] for rating_value in rating_col_vec]
    # ratings histogram
    get_hist(x=ratings_vec,
            xlabel="notes",
            ylabel="reccurence",
            plot_title="Distribution des notations des films dans la database movies",
            saving_path="../figures/hist_notes.png")

    # 1.4 get rating frequency table
    # building query to create the columns of the frequency tab
    rating_freq_tab_query = get_ratings_frequency_table_query(ratings_vec=ratings_vec)
    # executing ratings frequency tab query
    ratings_freq_tab = co.execute(rating_freq_tab_query).fetchall()
    # get the vector of ratings repartition in %
    ratings_freq_vec = [item for item in ratings_freq_tab[0]]

    # 2.1 create new empty column named 'liked' in ratings tab
    try:
        # add an empty 'liked' column
        cursor_obj.execute(add_empty_liked_col_query)
    except sqlite3.OperationalError:
        # Exception expected to occur: the liked column already added:
        # Nothing to worry about
        pass
    
    # fill the 'liked' column such as: 0 if rating [0,6]; 1 if rating [7, 10]
    cursor_obj.execute(fill_liked_col_query)

    # 2.2 get best rated movie genders 
    # get gender (combination) column and rating column
    gender_combin_with_ratings_tab = co.execute(gender_combin_with_ratings_query).fetchall()
    # get gender (not combination) ranked depending on the average rating score
    genders_avg_rating_df = get_rating_per_gender(gender_combin_with_ratings_tab)
    # get the top ten genders ranked depending on the average rating score
    top10_genders_list = genders_avg_rating_df.loc[:, 'genre'].to_list()[:10]

    # 3.1 top 10 most liked movies in the whole database
    top10_movies_list_with_avg_score = co.execute(top10_movies_query).fetchall()
    top10_movies_list = [item[0] for item in top10_movies_list_with_avg_score]

    # 3.2 top 10 most liked movies in 2020
    best_rated_movie_in_2020 = co.execute(best_rated_movie_in_2020_query).fetchall()[0][0]
    
    # 4.1 comparing performance of searching with and without index
    # index creation
    try:
        # try to create index for user_id in ratings table
        cursor_obj.execute(create_index_userid_query)
    except sqlite3.OperationalError:
        # exception expected to occur: idx_user_id already created
        pass

    # computation time spent for searching without indexation
    t0_classic_search = time()
    co.execute(search_without_index_query)
    tf_classic_search = time()
    perf_classic_search = "{:.2e}".format(tf_classic_search - t0_classic_search)
    
    # computation time spent for searching with indexation
    t0_index_search = time()
    co.execute(search_with_index_query)
    tf_index_search = time()
    perf_index_search = "{:.2e}".format(tf_index_search - t0_index_search)


# C. REPORT GENERATION

with open("../results.txt", "w", encoding="utf8") as f:
    # report title
    f.write("=== Résultats répondant aux énoncés du README.md === \n")
    # 1.1 nb of movies
    f.write(f"\n 1.1 {nb_films} films figurent dans la base de données.\n")
    # 1.2 nb of users 
    f.write(f"\n 1.2 {nb_users} users figurent dans la base de données.\n")
    # 1.3 distribution of ratings
    f.write("\n 1.3 la distribution des notes est representée par un histogramme "+
            "disponnible selon le chemin suivant: figures/hist_notes.png \n")
    # 1.4 frequency table for ratings
    f.write("\n 1.4 la répartition des notes dans la base de donnée est la suivante: \n")
    # frequency table column name
    f.write(f"|note|frequence| \n")
    for i, note_freq in enumerate(ratings_freq_vec):
        # add note and its frequency as a new row in the table
        tmp_note = i
        tmp_note_freq = ratings_freq_vec[i]
        f.write(f"| {tmp_note} | {tmp_note_freq}| \n")
    # 2.1
    f.write("\n 2.1 La colonne liked a été rajoutée dans la table ratings avec les valeurs suivantes: \n" +
            "- 0 pour les notes [0-6] \n- 1 pour les notes [7-10]. \n")
    # 2.2
    f.write("\n 2.2 le top 10 des genres les mieux notés en moyenne dans la database: \n")
    # intializing the table
    f.write(f"|rank|genre|\n")
    # filling the table
    for i, tmp_gender in enumerate(top10_genders_list):
        f.write(f"| {i + 1} | {tmp_gender} | \n")
    # 3.1
    f.write("\n 3.1 les 10 films les plus aimés par les internautes sont:\n")
    for film in top10_movies_list:
        f.write(f"| {film} |\n")
    # 3.2
    f.write(f"\n 3.2 le film le plus noté durant l'année 2020 est: {best_rated_movie_in_2020}.\n")
    # 4.1
    f.write(f"""
4.1 Nous voudrions comparer les performances entre la recherche par les id ou les index.
Notre stratégie consiste à comparer les temps des calcul de chaquene des deux méthodes.
Pour ce faire, nous enregistrons le temps initial (t0), puis on exécute la commande de recherche et 
on enregistre par la suite le temps final (tf). Le temps de calcul pour une méthode donnée est donc tf - t0.
Le temps de calcul enregistrée pour la recherche sans index est de {perf_classic_search}.
Le temps de calcul enregistrée pour la recherche avec index est de {perf_index_search}.""")
    if perf_classic_search > perf_index_search:
        f.write("La recherche par index a une meilleure performance en terme de temps de calcul.\n")
    elif perf_classic_search < perf_index_search:
        f.write("La recherche par index n'a pas une meilleure performance en terme de temps de calcul.\n")
    elif perf_classic_search == perf_index_search:
        f.write("Les deux méthodes ont la meme performance en terme de temps de calcul.\n")
    f.write("""
Remarque: Pour mesurer le temps de calcul, nous avons développé la fonction query_execution_computation_time().
Cependant, il y a une différence entre les résultats obtenus avec et sans l'utilisation de cette fonction.
Cela est probablement dû au temps supplémentaire ajouté pour l'exécution de la commande à l'intérieur de la fonction.
Par conséquent, nous avons choisi de ne prendre en compte que les temps de calcul calculés sans utiliser notre fonction.""")
