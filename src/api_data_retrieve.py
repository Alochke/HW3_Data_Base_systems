import pandas as pd
import csv
import create_db_script as cdbs
import os
import mysql

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
        for sub in x[array].split():
            ids.append(x[id])
            subs.append(array.replace('_', ' ')) # The dataset uses "_" to notate spaces within array's components.

    # Apply the parse function to each row of the DataFrame.
    base.apply(parse, axis=1)

    return pd.DataFrame({id: ids, array: subs})

def insert_data(cursor: mysql.connector.cursor_cext.CMySQLCursor):
  
  # This function executes the given sql command on the db, and it fills it correctly based on values from x, a df row.
  # We'll use it when going over the rows of a df and execute sql commands that are based on the rows.
  def exec(x: pd.core.series.Series, sql_str: str):
     cursor.execute(sql_str, x)
          
  # Creating the title table.
  temp = pd.read_csv(os.path.join("data", "title.basics.csv"),
                     dtype=  {'tconst': str, 'titleType': str, 'primaryTitle': str, 'originalTitle': str, 'isAdult': str, 'startYear': str, 'endYear': str, 'runtimeMinutes': str, 'genres': str},
                     quoting= csv.QUOTE_NONE)
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
  temp = pd.read_csv(os.path.join("data", "title.ratings.csv"), 
                     dtype={'tconst': str, 'averageRating': str, 'numVotes': str},
                     quoting= csv.QUOTE_NONE)
  update_title = (
    "UPDATE title "
    "SET ratings = %(avergeRating)s "
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
  temp.apply(lambda x: exec(x, add_genre))

  # Creating the title_person table.
  temp = pd.read_csv(os.path.join("data", "title.principals.csv"),
                     dtype={'tconst': str, 'ordering': str, 'nconst': str, 'category': str, 'job': str, 'characters': str})
  add_title_person = (
    "INSERT INTO title_person "
    "(temp1, temp2, job) "
    "VALUES(%(tconst)s, %(ncost)s, %(job)s)"
  )
  temp.apply(lambda x: exec(x, add_title_person), axis = 1)
  
  # Creating the person table.
  temp = pd.read_csv(os.path.join("data", "name.basics.csv"),
                     dtype=  {'nconst': str, 'primaryName': str, 'birthYear': str, 'deathYear': str, 'primaryProfession': str, 'knownForTitles': str},
                     quoting= csv.QUOTE_NONE)
  add_person = (
      "INSERT INTO person "
      "(temp, name) "
      "VALUES(%(nconst)s, %(primaryName)s)"
  )
  temp.apply(lambda x: exec(x, add_person), axis = 1)

  # Creating the profession table.
  profession_prep = create_df(temp, "nconst", "primaryProfession") 
  temp = profession_prep
  add_profession = (
    "INSERT INTO profession "
    "(temp, profession) "
    "VALUES(%(nconst)s, %(primaryProfession)s)"
  )
  temp.apply(lambda x: exec(x, add_profession))


  FIX_TABLES = {}
  FIX_TABLES['genre_update'] = (
      "UPDATE genre "
      "SET id = ( SELECT title.id "
                "FROM title "
                "WHERE genre.temp = title.temp )"
  )
  FIX_TABLES['genre_alter'] = (
      "ALTER TABLE genre "
      "ADD PRIMARY KEY (id, genre),"
      "ADD FOREIGN KEY (id) REFERENCES person(id),"
      "DROP COLUMN temp"
  )
  FIX_TABLES['title_person_update'] = (
      "UPDATE title_person "
      "SET title_id = ( SELECT title.id "
                      "FROM title "
                      "WHERE title_person.temp1 = title.temp ),"
      "SET person_id = ( SELECT person.id "
                      "FROM person "
                      "WHERE title_person.temp2 = person.id )"
  )
  FIX_TABLES['title_person_alter'] = (
      "ALTER TABLE title_person "
      "ADD PRIMARY KEY (title_id, person_id, job),"
      "ADD FOREIGN KEY (title_id) REFERENCES title(id),"
      "ADD FOREIGN KEY (person_id) REFERENCES person(id),"
      "DROP COLUMN temp1,"
      "DROP COLUMN temp2"
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
      "DROP COLUMN temp"
  )
  FIX_TABLES['title_alter'] = (
      "ALTER TABLE title "
      "DROP COLUMN temp,"
      "DROP COLUMN type,"
      "DROP COLUMN adult"
  )
  FIX_TABLES['person_alter'] = (
      "ALTER TABLE person "
      "DROP COLUMN temp"
  )

  for fix in FIX_TABLES:
        table_description = FIX_TABLES[fix]
        try:
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            print(err.msg)
            print(fix)

