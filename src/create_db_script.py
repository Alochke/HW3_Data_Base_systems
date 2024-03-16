import mysql.connector
from collections.abc import Iterable

TCONST_LEN = 10
NCONST_LEN = 10
MOVIE_LEN = 5
MAX_MOVIE_NAME_LEN = 208
MAX_MINUTES_LEN = 5
MAX_GENRE_LEN = 11 
MAX_JOB_LEN = 19
MAX_PERSON_NAME_LEN = 95 # It may be that the max length specified here is longer than the one found in the actual db,
# but it's true for insertion of all the rows of original dataset.
MAX_PROFESSION_LEN = 25
TITLE_ID_LEN = 6
PERSON_ID_LEN = 7
YEAR_LEN = 4
RATING_PRECISION = 2

def execute(cursor: mysql.connector.cursor_cext.CMySQLCursor ,iter: Iterable) -> None:
    """
    A refactoring function for executing the commands found in an iterable.

    Parameters:
        cursor (mysql.connector.cursor_cext.CMySQLCursor): The MySQL cursor object used for executing SQL commands.
        iter (Iterable): An iterable containing SQL commands to be executed.

    Returns:
        None

    Raises:
        None

    Notes:
        This function iterates through each SQL command in the provided iterable and executes it using the given cursor.
        If any error occurs during execution, it catches the `mysql.connector.Error` exception and prints the error message.

    Example:
        cursor = mysql_connection.cursor()
        sql_commands = ["INSERT INTO table1 (column1) VALUES ('value1')",
                        "UPDATE table2 SET column2 = 'new_value' WHERE condition",
                        "DELETE FROM table3 WHERE condition"]
        execute(cursor, sql_commands)
    """    
    for sql_str in iter:
        try:
            cursor.execute(sql_str)
        except mysql.connector.Error as err:
            print(err.msg)

def create_tables(cursor: mysql.connector.cursor_cext.CMySQLCursor) -> None:
    """
    Create tables in the database using the provided MySQL cursor.

    Parameters:
        cursor (mysql.connector.cursor_cext.CMySQLCursor): The MySQL cursor object used for executing SQL commands.

    Returns:
        None

    Raises:
        None

    Notes:
        This function creates tables in the database based on predefined schema specifications.
        It creates tables for storing movie titles, genres, persons, title-person relationships, and professions.
        Additional columns are temporarily added to tables to accommodate original IDs before being replaced with new shorter keys.
        Indexes and constraints are applied to ensure data integrity and optimize query performance.

    Example:
        cursor = mysql_connection.cursor()
        create_tables(cursor)
    """
    UPDATES = [] # This list accumulates the sql commands we'll use.
    
    # We add more columns than needed to all the tables, temporarly.
    # One of the principals behind it is the fact that the original dataset already comes with ids that
    # identify objects of interest, however all of those ids are 10 char long while 32 bits are more than
    # enough to identify the objects the original IDs identify in our reduced data (even if we won't reduce each csv to 50,000 rows)
    # therefore we use the original IDs only as temporaries and let the db generate new shorter keys,
    # we need to save the temporaries until spreading the new IDs across all tables to preserve
    # consistency across all tables.
    UPDATES.append((
        "CREATE TABLE title("
        f"id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,"
        f"temp CHAR({TCONST_LEN}) NOT NULL,"
        f"type CHAR({MOVIE_LEN}) NOT NULL,"
        f"name VARCHAR({MAX_MOVIE_NAME_LEN}) NOT NULL,"
        "adult BOOL NOT NULL,"
        f"year SMALLINT({YEAR_LEN}) UNSIGNED NOT NULL," # We use the smallint type and not a year type because a year type has a lower bound of 1901, while there are movies that were produced before 1901.
        f"minutes SMALLINT({MAX_MINUTES_LEN}) UNSIGNED NOT NULL,"
        "ratings FLOAT({RATING_PRECISION}) DEFAULT 0 NOT NULL,"
        "PRIMARY KEY (id),"
        "CONSTRAINT adult_check CHECK (adult = 0),"
        "CONSTRAINT type_check CHECK (type LIKE 'movie')" # We want our data to be reduced to movies.
        ") ENGINE=InnoDB"
    ))
    UPDATES.append((
        "CREATE INDEX temp1 ON title(temp)" # We have add this index for the next table creation to work properly.
    ))
    UPDATES.append((
        "CREATE TABLE genre("
        f"id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0," # ids will be updated later.
        f"temp char({TCONST_LEN}) NOT NULL," 
        f"genre varchar({MAX_GENRE_LEN}) NOT NULL,"
        "CONSTRAINT genre_key FOREIGN KEY (temp) REFERENCES title(temp)" # We limit the table to movies by this key.
        ") ENGINE=InnoDB"
    ))
    UPDATES.append((
        "CREATE TABLE person("
        f"id MEDIUMINT({PERSON_ID_LEN}) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,"
        f"temp char({NCONST_LEN}) NOT NULL,"
        f"name varchar({MAX_PERSON_NAME_LEN}) NOT NULL,"
        "PRIMARY KEY (id)"
        ") ENGINE=InnoDB"
    ))
    UPDATES.append((
        "CREATE INDEX temp2 ON person(temp)" # We have add this index for the next table creation to work properly.
    ))
    UPDATES.append((
        "CREATE TABLE title_person("
        f"title_id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"person_id MEDIUMINT({PERSON_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"temp1 char({TCONST_LEN}) NOT NULL,"
        f"temp2 char({NCONST_LEN}) NOT NULL,"
        f"job varchar({MAX_JOB_LEN}) NOT NULL,"
        "CONSTRAINT title_person_title FOREIGN KEY (temp1) REFERENCES title(temp)," # We limit the data to persons from the movie industry by this key.
        "CONSTRAINT title_person_person  FOREIGN KEY (temp2) REFERENCES person(temp)"
        ") ENGINE=InnoDB"
    ))
    UPDATES.append((
        "CREATE TABLE profession("
        f"id MEDIUMINT({PERSON_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"temp char({NCONST_LEN}) NOT NULL,"
        f"profession varchar({MAX_PROFESSION_LEN}) NOT NULL,"
        "CONSTRAINT profession_key FOREIGN KEY (temp) REFERENCES person(temp)" # We limit the data to persons from the movie industry by this key.
        ") ENGINE=InnoDB"
    ))
    UPDATES.append((
        "CREATE INDEX title_year ON title(year) USING BTREE"
    ))
    UPDATES.append((
        "ALTER TABLE title ADD FULLTEXT(name)"
    ))
    UPDATES.append((
        "ALTER TABLE genre ADD FULLTEXT(genre)"
    ))
    UPDATES.append((
        "ALTER TABLE person ADD FULLTEXT(name)"
    ))
    
    execute(cursor, UPDATES)
