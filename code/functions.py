"""Functions needed to manipulate the ../code/movies.sqlite database."""

import time
import matplotlib.pyplot as plt
import pandas as pd
import datetime


def get_hist(x, xlabel, ylabel, plot_title, saving_path):
    """Generate a histogram and save it as a png file

    args:
        x (list): variable to explore.
        xlabel (str): label for the x-axis.
        ylabel (str): label for the y-axis.
        plot_title (str): title of the histogram.
        saving_path (str): path to save the generated histogram.
    """

    plt.figure()
    plt.hist(x)
    plt.title(plot_title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)    
    plt.savefig(saving_path)
    # close the plot to free up memory
    plt.close()


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
