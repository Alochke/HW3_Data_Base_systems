import queries_execution as qe
import pandas as pd
import csv
import create_db_script as cdbs

def create_genre() -> pd.DataFrame:
    """
    Creates a dataframe that represents the genres of movies.
    
    Returns:
    A pd.DataFrame. DataFrame containing relevant information from data/title.basics.csv.
        The dataframe has two columns, id, a unique identifier of a title and genre, the genre of the title.

    Note:
    - This function assumes the existence of 'data' directory in the current working directory, containing the used csvs.

    Example:
    genre = create_genre()
    """
    titles = pd.read_csv(os.path.join("data", "title.basics.csv"),
                            dtype=  {'tconst': str, 'titleType': str, 'primaryTitle': str, 'originalTitle': str, 'isAdult': int, 'startYear': str, 'endYear': str, 'runtimeMinutes': str, 'genres': str},
                            quoting= csv.QUOTE_NONE)


    # Initialize lists to store ids and genres
    ids = []
    genres = []

    # Define a function to add genres for each row in the DataFrame
    def add_genre(x: pd.core.series.Series):
        for genre in x['genres'].split():
            ids.append(x['tconst'])
            genres.append(genre) # The dataset uses "_" to notate spaces within professions.

    # Apply the add_genre function to each row of the DataFrame
    titles.apply(add_genre, axis=1)

    return pd.DataFrame({"id": ids, "genre": genres})

def create_proffession() -> pd.DataFrame:
    """
    Creates a data frame equivalent of the proffession table.
    
    Returns:
    A pd.DataFrame. DataFrame containing relevant information from data/name.basics.csv,
        The dataframe has two columns, id, a unique identifier of a person and profession, one of the professions of a person.

    Note:
    - This function assumes the existence of 'data' directory in the current working directory, containing the used csvs.

    Example:
    genre = create_genre()
    """
    names = pd.read_csv(os.path.join("data", "name.basics.csv"),
                        dtype={'nconst': str, 'primaryName': str, 'birthYear': str, 'deathYear': str, 'primaryProfession': str, 'knownForTitles': str}, 
                        quoting = csv.QUOTE_NONE)

    # Initialize lists to store ids and genres
    ids = []
    professions = []

    # Define a function to add genres for each row in the DataFrame
    def add_profession(x: pd.core.series.Series):
        # Some rows have the type of the 'primaryProfession' column as float which will result in an error, that's because the matching csv field
        # is empty, so we drop their data, because it's not of any significance anyways.
        if type(x['primaryProfession']) == str:
                for genre in x['primaryProfession'].split():
                    ids.append(x['nconst'])
                    professions.append(genre.replace("_", " ")) # The dataset uses "_" to notate spaces within professions.

    # Apply the add_genre function to each row of the DataFrame
    names.apply(add_profession, axis=1)

    return pd.DataFrame({"id": ids, "profession": professions})

def insert_tables():
  # Prepare dfs for insertion.
  dfs = {}
  dfs['title'] = pd.read_csv(os.path.join("data", "title.basics.csv"),
                            dtype={'tconst': str, 'titleType': str, 'primaryTitle': str, 'originalTitle': str, 'isAdult': str, 'startYear': str, 'endYear': str, 'runtimeMinutes': str, 'genres': str},
                            quoting= csv.QUOTE_NONE)
  dfs['genre'] = cdbs.create_genre()
  dfs['person'] = pd.read_csv(os.path.join("data", "name.basics.csv"),
                              dtype={'nconst': str, 'primaryName': str, 'birthYear': str, 'deathYear': str, 'primaryProfession': str, 'knownForTitles': str},
                              quoting= csv.QUOTE_NONE)
  dfs['title_person'] = pd.read_csv(os.path.join("data", "title.principals.csv"),
                                    dtype={'tconst': str, 'ordering': str, 'nconst': str, 'category': str, 'job': str, 'characters': str},
                                    quoting= csv.QUOTE_NONE)
  dfs['profession'] = cdbs.create_proffession()

if __name__ == "__main__":
  qe.cnx.close()
