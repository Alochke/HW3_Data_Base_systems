import mysql.connector

TCONST_LEN = 10
NCONST_LEN = 10
MOVIE_LEN = 5
MAX_MOVIE_NAME_LEN = 208
MAX_MINUTES_LEN = 5
MAX_GENRE_LEN = 11 
MAX_JOB_LEN = 19
MAX_PERSON_NAME_LEN = 95
MAX_PROFESSION_LEN = 25
TITLE_ID_LEN = 6
PERSON_ID_LEN = 7



def create_tables(cursor: mysql.connector.cursor_cext.CMySQLCursor):
    """
    This function creates the db.
    """
    UPDATES = [] # This list accumulates the sql commands we'll use.
    
    # We add more columns than needed to all the tables, temporarly.
    # One of the principals behind it is the fact that the original dataset already comes with ids that
    # identify objects of interest, however all of those ids are 10 char long while 32 bits are more than
    # enough to identify the objects the original IDs identify in our reduced data,
    # therefore we use the original IDs only as temporaries and let the db generate new shorter keys,
    # we need to preserve the temporaries until spreading the new IDs across all tables to preserve
    # consistency across all tables.
    # We also use them to filter unused data, by utilizing foreign keys.
    # For example, the original dataset contains data about video game directors unassociated with the movie
    # industry, we get rid of those by making a person's tempory id be a foreign key refrencing tempory IDs
    # of persons appearing in the title_person table, we know for sure that only IDs of persons associated with
    # the movies industry can appear there because its rows were filtered priorly by making sure the tempory
    # title_id of those rows appear in the title table, via a foreign key constraint.
    UPDATES.append((
        "CREATE TABLE title("
        f"id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,"
        f"temp CHAR({TCONST_LEN}) NOT NULL,"
        f"type CHAR({MOVIE_LEN}) NOT NULL,"
        f"name VARCHAR({MAX_MOVIE_NAME_LEN}) NOT NULL,"
        "adult BOOL NOT NULL,"
        "year SMALLINT(4) UNSIGNED NOT NULL," # We use the smallint type and not a year type because a year type has a lower bound of 1901, while there are movies that were produced before 1901.
        f"minutes SMALLINT({MAX_MINUTES_LEN}) UNSIGNED NOT NULL,"
        "ratings FLOAT(2) DEFAULT 0 NOT NULL,"
        "PRIMARY KEY (id),"
        "CONSTRAINT year_lower CHECK (year <= 2024),"
        "CONSTRAINT adult_check CHECK (adult = 0)," # No sexy content sneaking into our db, naughty naughty!
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
        "PRIMARY KEY (id),"
        "CONSTRAINT person_key FOREIGN KEY (temp) REFERENCES title_person(temp2)" # We limit the data to persons from the movie industry by this key.
        ") ENGINE=InnoDB"
    ))
    UPDATES.append((
        "CREATE TABLE title_person("
        f"title_id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"person_id MEDIUMINT({PERSON_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"temp1 char({TCONST_LEN}) NOT NULL,"
        f"temp2 char({NCONST_LEN}) NOT NULL,"
        f"job varchar({MAX_JOB_LEN}) NOT NULL,"
        "CONSTRAINT title_person_key FOREIGN KEY (temp1) REFERENCES title(temp)" # We limit the data to persons from the movie industry by this key.
        ") ENGINE=InnoDB"
    ))
    UPDATES.append((
        "CREATE INDEX temp ON title_person(temp2)" # We have to add this index for the next table creation to work, we'll remove it after data insertion.
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
    
    for sql_str in UPDATES:
        try:
            cursor.execute(sql_str)
        except mysql.connector.Error as err:
            print(err.msg)
            print(sql_str)

