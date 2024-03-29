import pandas as pd
import csv
import os
import mysql
from create_db_script import execute

def create_df(base: pd.DataFrame, id: str, array: str) -> pd.DataFrame:
    """
    This function serves as a refactorization of the process of parsing the primaryProffession column of data/name.basics.csv
    and the genres column of data/title.basics.csv for the creation of the profession table and the genre table, respectively,
    in the db.
    In some rows, those columns in the original dataset have a banch of values stored together in the same column,
    this function seprates the values.

    Parameters:
      base (pd.DataFrame): A dataframe that is simply the csv the data is parsed from.
      id (str): The name of the column in the original database that has the ID of the rows.
      array (str): The name of the column in the original database that has multiple data in some rows.

    
    Returns (pd.DataFrame):
      A dataframe that has the data parsed, it will have two columns, one containts the id of the row the data was parsed from and its ids,
      the other has the seprated data that was compressed into one cell.
      Each of the columns will be named as the column it was taken from.

    Example:

    """

    ids = []
    subs = []

    # Define a function to preform on each row in base.
    def parse(x: pd.core.series.Series):
        if type(x[array]) == str: # We add this check to handle \N in the data set (and to not insert them into the db.).
          for sub in x[array].split():
              ids.append(x[id])
              subs.append(sub.replace('_', ' ')) # The dataset uses "_" to notate spaces within array's components.

    # Apply the parse function to each row of the DataFrame.
    base.apply(parse, axis=1)

    return pd.DataFrame({id: ids, array: subs})

def insert_data(cursor: mysql.connector.cursor_cext.CMySQLCursor):
    """
    Execute SQL commands to insert data into the database tables.

    Parameters:
        cursor (mysql.connector.cursor_cext.CMySQLCursor): The MySQL cursor object used for executing SQL commands.

    Returns:
        None

    Raises:
        None

    Notes:
        This function populates the database tables with data extracted from CSV files.
        It reads data from multiple CSV files, performs necessary data transformations, and inserts the data into respective tables.
        Various SQL commands are executed to insert data, update tables, and perform necessary fixes to ensure data consistency.

    Example:
        cursor = mysql_connection.cursor()
        insert_data(cursor)
    """
    
    
    # This function executes the given sql command on the db, and it fills it correctly based on values from x, a df row.
    # We'll use it when going over the rows of a df and execute sql commands that are based on the rows.
    def exec(x: pd.core.series.Series, sql_str: str):
      try:
        cursor.execute(sql_str, x.to_dict())
      except mysql.connector.Error as err:
        None

            
    # Creating the title table.
    temp = pd.read_csv(os.path.join("..", "data", "title.basics.csv"),
                      dtype=  {'tconst': str, 'titleType': str, 'primaryTitle': str, 'originalTitle': str, 'isAdult': str, 'startYear': str, 'endYear': str, 'runtimeMinutes': str, 'genres': str},
                      quoting= csv.QUOTE_NONE, # We don't pandas parse quets as escape chacracter.
                      na_values= ['\\N'] # We want to drop all \Ns.
                      )

    add_title = (
        "INSERT INTO title "
        "(temp, type, name, adult, year, minutes) "
        "VALUES(%(tconst)s, %(titleType)s, %(primaryTitle)s, %(isAdult)s, %(startYear)s, %(runtimeMinutes)s)"
    )
    temp.apply(lambda x: exec(x, add_title), axis = 1)
    # I intentionally prepare the df I need for the genres column beforehand, 
    # because I don't want to call pd.read_csv more times than necessary, it's a heavy operation
    # and I also don't want to keep a variable pointing to temp, to free up memory. 
    genres_prep = create_df(temp, "tconst", "genres") 
    temp = pd.read_csv(os.path.join("..", "data", "title.ratings.csv"), 
                      dtype={'tconst': str, 'averageRating': float, 'numVotes': str},
                      )
    update_title = (
      "UPDATE title "
      "SET ratings = %(averageRating)s "
      "WHERE temp = %(tconst)s"
    )
    temp.apply(lambda x: exec(x, update_title), axis=1)

    # Creating the genres table.
    temp = genres_prep
    add_genre = (
      "INSERT INTO genre "
      "(temp, genre) "
      "VALUES(%(tconst)s, %(genres)s)"
    )
    temp.apply(lambda x: exec(x, add_genre), axis=1)


    
    # Creating the person table.
    temp = pd.read_csv(os.path.join("..", "data", "name.basics.csv"),
                      dtype=  {'nconst': str, 'primaryName': str, 'birthYear': str, 'deathYear': str, 'primaryProfession': str, 'knownForTitles': str},
                      quoting= csv.QUOTE_NONE,
                      keep_default_na = False,
                      na_values= ['\\N']
                      )
    add_person = (
        "INSERT INTO person "
        "(temp, name) "
        "VALUES(%(nconst)s, %(primaryName)s)"
    )
    # Preparing the profession tables beforehand to minimaize memory usage, we want the garbage collector to work.
    profession_prep = create_df(temp, "nconst", "primaryProfession") 
    temp.apply(lambda x: exec(x, add_person), axis = 1)

    # Creating the title_person table.
    temp = pd.read_csv(os.path.join("..", "data", "title.principals.csv"),
                      dtype={'tconst': str, 'ordering': str, 'nconst': str, 'category': str, 'job': str, 'characters': str},
                      quoting= csv.QUOTE_NONE,
                      na_values= ['\\N'])
    add_title_person = (
      "INSERT INTO title_person "
      "(temp1, temp2, job) "
      "VALUES(%(tconst)s, %(nconst)s, %(category)s)"
    )
    temp.apply(lambda x: exec(x, add_title_person), axis = 1)

    # Creating the profession table.
    temp = profession_prep
    add_profession = (
      "INSERT INTO profession "
      "(temp, profession) "
      "VALUES(%(nconst)s, %(primaryProfession)s)"
    )
    temp.apply(lambda x: exec(x, add_profession), axis = 1)
  


    FIX_TABLES = {}
    FIX_TABLES['profession_fix'] = (
      "DELETE FROM profession "
      "WHERE profession.temp NOT IN ( SELECT title_person.temp2 "
                                      "FROM title_person )"
      )
    FIX_TABLES['person_fix'] = (
      "DELETE FROM person "
      "WHERE person.temp NOT IN ( SELECT title_person.temp2 "
                                  "FROM title_person )"
      )
    FIX_TABLES['profession_update'] = (
      "UPDATE profession "
      "SET id = ( SELECT person.id "
                  "FROM person "
                  "WHERE profession.temp = person.temp )"
      )
    FIX_TABLES['profession_alter'] = (
      "ALTER TABLE profession "
      "ADD PRIMARY KEY (id, profession),"
      "ADD FOREIGN KEY (id) REFERENCES person(id),"
      "DROP FOREIGN KEY profession_key,"
      "DROP COLUMN temp"
      )
    FIX_TABLES['title_person_update1'] = (
      "UPDATE title_person "
      "SET title_id = ( SELECT title.id "
                        "FROM title "
                        "WHERE title_person.temp1 = title.temp )"
      )
    FIX_TABLES['title_person_update2'] = (
      "UPDATE title_person "
      "SET person_id = ( SELECT person.id "
                        "FROM person "
                        "WHERE title_person.temp2 = person.temp )"
      )
    FIX_TABLES['title_person_alter'] = (
      "ALTER TABLE title_person "
      "ADD PRIMARY KEY (title_id, person_id, job),"
      "ADD FOREIGN KEY (title_id) REFERENCES title(id),"
      "ADD FOREIGN KEY (person_id) REFERENCES person(id),"
      "DROP FOREIGN KEY title_person_person,"
      "DROP FOREIGN KEY title_person_title,"
      "DROP COLUMN temp1,"
      "DROP COLUMN temp2"
      )
    FIX_TABLES['person_alter'] = (
      "ALTER TABLE person "
      "DROP COLUMN temp"
      )
    FIX_TABLES['genre_update'] = (
      "UPDATE genre "
      "SET id = ( SELECT title.id "
      "FROM title "
      "WHERE genre.temp = title.temp )"
      )
    FIX_TABLES['genre_alter'] = (
      "ALTER TABLE genre "
      "ADD PRIMARY KEY (id, genre),"
      "ADD FOREIGN KEY (id) REFERENCES title(id),"
      "DROP FOREIGN KEY genre_key,"
      "DROP COLUMN temp"
      )
    FIX_TABLES['title_alter'] = (
      "ALTER TABLE title "
      "DROP COLUMN temp,"
      "DROP COLUMN type,"
      "DROP COLUMN adult"
      )
    
    execute(cursor, FIX_TABLES.values())
