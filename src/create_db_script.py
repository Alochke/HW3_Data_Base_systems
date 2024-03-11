import os
import pandas as pd
from typing import Tuple

def create_base() -> Tuple[pd.DataFrame]:
    """
    This filters unneeded data. It's a refactoring function so we won't have to filter the data twice, when creating the title data frame and when
    creating the episode data frame.
    It's returned value should be passed to create_title and to create_episode.

    Returns (Tuple[pd.DataFrame]):
        A tuple containing two DataFrames - 'basics' and 'ratings'.

    The 'basics' DataFrame contains information about titles, including a unique id, type (short, movie, video etc.), name, whether it is adult content,
    the year at which the title production started and duration.
    The 'ratings' DataFrame contains information about the average rating and the unique id of each title.

    Note:
    - The function assumes the existence of 'title.basics.csv' and 'title.ratings.csv' files in the 'data' directory.

    Example:
    basics_df, ratings_df = create_base()
    """
    # Reads the 'title.basics.csv' and 'title.ratings.csv' files located in the 'data' directory.
    basics = pd.read_csv(os.path.join("data", "title.basics.csv") \
                         , dtype={'tconst': str, 'titleType': str, 'primaryTitle': str, 'originalTitle': str, 'isAdult': str, 'startYear': str, 'endYear': str, 'runtimeMinutes': str, 'genres': str})
    ratings = pd.read_csv(os.path.join("data", "title.ratings.csv") \
                          , dtype={'tconst': str, 'averageRating': float, 'numVotes': int})

    # Drops unnecessary data.
    basics = basics[basics['isAdult'].isin(['0', '1'])] # We don't want any data ambivalent about being sexual sneaking into our data base. naughty, naughty!
    basics.drop(inplace= True, columns= ["originalTitle", "endYear", "genres"])
    ratings.drop(inplace = True, columns= ["numVotes"])

    return basics, ratings

def create_title(basics: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a data frame equivelent of the title table,
    The only worth to mention diffrence is that the columns of the data frame have the name of the column they are based upon in the original datasets.

    Parameters:
        basics (pd.DataFrame): The first component of the tuple returned by create_base.
        ratings (pd.DataFrame): The second component of the tuple returned by create_base.
    
    Returns:
    A pd.DataFrame. Merged DataFrame containing relevant information from the two given data frames.

    Example:
    print(create_title(*create_base()))
    """
    temp = basics[~basics["titleType"].isin(['videoGame', 'tvPilot', 'tvSpecial', 'tvEpisode'])]

    # Merges the processed DataFrames based on the 'tconst' column.
    return pd.merge(temp, ratings, on = "tconst")

def create_episode(basics: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a data frame equivelent of the episode table,
    The only worth to mention diffrence is that the columns of the data frame have the name of the column they are based upon in the original datasets.

    Parameters:
        basics (pd.DataFrame): The first component of the tuple returned by create_base.
        ratings (pd.DataFrame): The second component of the tuple returned by create_base.
    
    Returns:
    A pd.DataFrame. Merged DataFrame containing relevant information from the two given data frames.

    Example:
    print(create_episode(*create_base()))
    """
    episode = pd.read_csv(os.path.join("data", "title.episode.csv") \
                         , dtype={'tconst': str, 'parentTconst': str, 'seasonNumber': str, 'episodeNumber': str})

    temp = pd.merge(basics, episode, on = "tconst")
    return pd.merge(temp, ratings, on = "tconst")

def create_genre(tconsts: pd.core.series.Series) -> pd.DataFrame:
    """
    Creating a data frame equivelent of the genre table,
    The only worth to mention diffrence is that the columns of the data frame have the name of the column they are based upon in the original data set.

    Parameters:
        tconsts (pd.core.series.Series):
            Should be the the union of the "tconst" columns of the data frames returned by create_episode and create_title,
            will be used to drop rows that won't be used in the db,
            becuase we don't store data about genres of titles we don't store data about in the title table or the episode table.
    
    Returns:
    A pd.DataFrame. DataFrame containing relevant information from data/title.basics.csv.

    Note:
    - This function assumes the existence of 'data' directory in the current working directory, containing the used csvs.

    Example:
    episode = create_episode(*create_base())
    title = create_title(*create_base())
    create_genre(pd.concat([episode[["tconst"]], title[["tconst"]]])["tconst"])
    """
    genres_df = pd.read_csv(os.path.join("data", "title.basics.csv"),
                            dtype={'tconst': str, 'titleType': str, 'primaryTitle': str, 'originalTitle': str, 'isAdult': str, 'startYear': str, 'endYear': str, 'runtimeMinutes': str, 'genres': str})

    # Select only the relevant columns ('tconst' and 'genres')
    genres_df = genres_df[['tconst', 'genres']]

    # Filter rows based on the provided tconsts
    genres_df = genres_df[genres_df['tconst'].isin(tconsts)]

    # Initialize lists to store tconsts and genres
    ids = []
    genres = []

    # Define a function to add genres for each row in the DataFrame
    def add_genre(x: pd.core.series.Series):
        for genre in x['genres'].split():
            ids.append(x['tconst'])
            genres.append(genre)

    # Apply the add_genre function to each row of the DataFrame
    genres_df.apply(add_genre, axis=1)

    return pd.DataFrame({"tconst": ids, "genres": genres})

def create_title_person(tconsts: pd.core.series.Series):
    """
    Creating a data frame equivelent of the title_person table,
    The only worth to mention diffrence is that the columns of the data frame have the name of the column they are based upon in the original data set.

    Parameters:
        tconsts (pd.core.series.Series):
            Should be the the union of the "tconst" columns of the data frames returned by create_episode and create_title,
            will be used to drop rows that won't be used in the db,
            becuase we don't store data about titles we don't store data about in the title table or the episode table.
    
    Returns:
    A pd.DataFrame. DataFrame containing relevant information from data/title.principals.csv.

    Note:
    - This function assumes the existence of 'data' directory in the current working directory, containing the used csvs.

    Example:
    episode = create_episode(create_base())
    title = create_title(create_base())
    create_title_person(pd.concat([episode[["tconst"]], title[["tconst"]]])["tconst"])
    """
    title_person = pd.read_csv(os.path.join("data", "title.principals.csv") \
                         , dtype={'tconst': str, 'ordering': int, 'nconst': str, 'category': str, 'job': str, 'characters': str})

    title_person = title_person[['tconst', 'nconst', 'category']]
    title_person = title_person[title_person['tconst'].isin(tconsts)]

    return title_person

# def create_person(tconsts: pd.core.series.Series):
#     """
#     Creating a data frame equivelent of the title_person table,
#     The only worth to mention diffrence is that the columns of the data frame have the name of the column they are based upon in the original data set.

#     Parameters:
#         tconsts (pd.core.series.Series):
#             Should be the the union of the "tconst" columns of the data frames returned by create_episode and create_title,
#             will be used to drop rows that won't be used in the db,
#             becuase we don't store data about titles we don't store data about in the title table or the episode table.
    
#     Returns:
#     A pd.DataFrame. DataFrame containing relevant information from data/title.principals.csv.

#     Note:
#     - This function assumes the existence of 'data' directory in the current working directory, containing the used csvs.

#     Example:
#     episode = create_episode(create_base())
#     title = create_title(create_base())
#     create_title_person(pd.concat([episode[["tconst"]], title[["tconst"]]])["tconst"])
#     """
#     person = pd.read_csv(os.path.join("data", "name.basics.csv") \
#                          , dtype={'nconst': str, 'primaryName': int, 'birthYear': int, 'deathYear': int, 'primaryProfession': str, 'knownForTitles': str})

#     person = person[['nconst', 'nconst', 'category']]
#     title_person = title_person[title_person['tconst'].isin(tconsts)]

#return title_person

if __name__ == "__main__":
    base = create_base()
    # title = create_title(*base)
    # print(title)
    # episode = create_episode(*base)
    # print(episode)
    # join = pd.concat([episode[["tconst"]],title[["tconst"]]])
    # base = None
    # episode = None
    genre = create_genre(base[0]["tconst"])
    print(genre)
    title_person = create_title_person(base[0]["tconst"])
    print(title_person)
