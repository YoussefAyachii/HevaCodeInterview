"""Functions needed to manipulate the ../data/movies.sqlite database."""

import time
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime


def get_hist(x, xlabel, ylabel, plot_title, saving_path):
    """Generate a histogram and save it as a png file

    Args:
        x (list): variable to explore.
        xlabel (str): label for the x-axis.
        ylabel (str): label for the y-axis.
        plot_title (str): title of the histogram.
        saving_path (str): path to save the generated histogram.
    
    Returns:
        None
    """

    plt.figure()
    plt.hist(x)
    plt.title(plot_title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)    
    plt.savefig(saving_path)
    # close the plot to free up memory
    plt.close()


def get_ratings_frequency_table_query(ratings_vec):
    """Builds a query to create a ratings frequency table in SQL.

    Args:
        ratings_vec (list): A list of ratings values.

    Returns:
        str: The query string to create the ratings frequency table.
    """
    # min and max of ratings
    min_ratings, max_ratings = np.min(ratings_vec), np.max(ratings_vec)
    # build the query to create the columns of the frequency table
    query = ""
    for note in range(min_ratings, max_ratings + 1):
        query += f"ROUND((SUM(CASE WHEN rating = {note} THEN 1 ELSE 0 END) * 100.0 / COUNT(rating)), 2) AS prop_{note}"
        if note != max_ratings:
            query += ","

    # build the ratings frequency table query
    ratings_freq_tab_query = f"SELECT {query} FROM ratings"

    return ratings_freq_tab_query


def get_rating_per_gender(gender_combin_with_ratings_tab):
    """Transform a ratings by gender-combination table to ratings by single gender table.

    Args:
    gender_combin_with_ratings_tab (list): list of tuples containing gender combinations and their respective ratings.
        Each tuple should have a gender combination as a string where genders are separated by the '|' character,
        and a rating as a numerical value.

    Returns:
    genders_ranking_df (DataFrame): pandas DataFrame containing the average ratings per gender.
        It has two columns: 'genre' for the gender and 'avg_rating' for the average rating.
    """

    # get the gender combination list and their raitngs
    gender_combin_list = [item[0] for item in gender_combin_with_ratings_tab]
    gender_combin_ratings = [item[1] for item in gender_combin_with_ratings_tab]

    # output initialization
    gender_list, gender_ratings = [], []
    
    for i, gender_combin in enumerate(gender_combin_list):
        # verification of empty elements
        assert gender_combin is not None, "elements in gender_combin_list cannot be None"
        assert gender_combin_ratings[i] is not None, "elements in gender_combin_ratings cannot be None"
    
        tmp_gender_list = gender_combin.split("|")
        tmp_rating_list = [gender_combin_ratings[i] for j in range(len(tmp_gender_list))]
        
        # append
        gender_list += tmp_gender_list
        gender_ratings += tmp_rating_list

    gender_ratings_tab = pd.DataFrame({"genre":gender_list, "rating":gender_ratings})

    # compute the average rating by gender
    genders_rating_df = gender_ratings_tab.groupby('genre')['rating'].mean().reset_index()
    genders_rating_df.columns = ['genre', 'avg_rating']  # rename columns
    
    # sort genders by rating
    genders_ranking_df = genders_rating_df.sort_values(by='avg_rating', ascending=False)

    return genders_ranking_df


def convert_to_unix_time(time_str, format_str):
    """Converts a time string to Unix timestamp.

    Args:
        time_str (str): the time string to convert.
        format_str (str): the format string specifying the format of the input time string.

    Returns:
        unix_time (int): the Unix timestamp representing the input time.

    Example:
        time_str = "2023:07:14:12:34:56"
        format_str = "%Y:%m:%d:%H:%M:%S"
        unix_time = convert_to_unix_time(time_str, format_str)
        print(unix_time)
        # Output: 1696303696

    Note:
        The function assumes that the input time string and format string are valid and compatible.
    """
    # Create a datetime object from the input time string and format
    time_obj = datetime.datetime.strptime(time_str, format_str)
    
    # Convert the datetime object to Unix timestamp
    unix_time = int(time_obj.timestamp())
    
    return unix_time


def query_execution_computation_time(query, sqlite3_connect_obj,
                                    rounded=True, nb_val_after_comas=2):
    """Measure the computation time of executing an SQL query and format it in scientific notation.

    Args:
        query (str): the SQL query to be executed.
        sqlite3_connect_obj (sqlite3.Connection): the SQLite3 connection object.
        rounded (bool, optional): whether to round the computation time and format it in scientific notation.
                                  Defaults to True.
        nb_val_after_comas (int, optional): the number of decimal places to display in scientific notation.
                                            Defaults to 2.

    Returns:
        comp_time (str or float): the computation time in scientific notation (str) if rounded is True,
                      or the raw computation time (float) if rounded is False.
    """
    t0 = time.time()
    sqlite3_connect_obj.execute(query)
    tf = time.time()
    # Computation time of the query
    comp_time = tf - t0
    # Convert comp_time to 10e format for convenience
    if rounded:
        comp_time = "{:.{}e}".format(comp_time, nb_val_after_comas) 
    return comp_time

def write_row_by_row_tab(file_obj, col1, col2, col1_name, col2_name):
    """
    Writes a table with two columns, row by row, into the specified file object.

    Args:
        file_obj (file object): The file object to write the table into.
        col1 (list): The values of the first column.
        col2 (list): The values of the second column.
        col1_name (str): The name of the first column.
        col2_name (str): The name of the second column.

    Raises:
        AssertionError: If the lengths of col1 and col2 are not the same.

    Returns:
        None
    """
    # verify col1 and col2 are of the same length
    assert len(col1) == len(col2), "columns are not of the same length"
    # tab initialization: column name
    nb_rows = len(col1)
    # first row: column names
    file_obj.write(f"|{col1_name}|{col2_name}|\n")
    for i in range(nb_rows):
        # add note and its frequency as a new row in the table
        tmp_col1_value = col1[i]
        tmp_col2_value = col2[i]
        file_obj.write(f"| {tmp_col1_value} | {tmp_col2_value} | \n")
