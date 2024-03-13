import mysql

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
    TABLES = {}
    TABLES['title'] = (
        "CREATE TABLE title("
        f"id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,"
        f"temp CHAR({TCONST_LEN}) NOT NULL UNIQUE,"
        f"type CHAR({MOVIE_LEN}) NOT NULL,"
        f"name VARCHAR({MAX_MOVIE_NAME_LEN}) NOT NULL,"
        "adult BOOL NOT NULL,"
        "year YEAR NOT NULL,"
        f"minutes SMALLINT({MAX_MINUTES_LEN}) UNSIGNED NOT NULL,"
        "ratings FLOAT(2) DEFAULT 0 NOT NULL,"
        "PRIMARY KEY (id),"
        "CONSTRAINT type_check CHECK (type LIKE 'movie')"
        ") ENGINE=InnoDB"
    )
    TABLES['genre'] = (
        "CREATE TABLE genre("
        f"id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"temp char({TCONST_LEN}) NOT NULL,"
        f"genre varchar({MAX_GENRE_LEN}) NOT NULL,"
        "FOREIGN KEY (temp) REFERENCES title(temp)"
        ") ENGINE=InnoDB"
    )
    TABLES['title_person'] = (
        "CREATE TABLE title_person("
        f"film_id MEDIUMINT({TITLE_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"person_id MEDIUMINT({PERSON_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"temp1 char({TCONST_LEN}) NOT NULL,"
        f"temp2 char({NCONST_LEN}) NOT NULL,"
        f"job varchar({MAX_JOB_LEN}) NOT NULL,"
        "FOREIGN KEY (temp1) REFERENCES title(temp)"
        ") ENGINE=InnoDB"
    )
    TABLES['person'] = (
        "CREATE TABLE person("
        f"person_id MEDIUMINT({PERSON_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"temp char({NCONST_LEN}) NOT NULL,"
        f"name varchar({MAX_PERSON_NAME_LEN}) NOT NULL,"
        "FOREIGN KEY (temp) REFERENCES title_person(temp2)"
        ") ENGINE=InnoDB"
    )
    print(TABLES['person'])
    TABLES['profession'] = (
        "CREATE TABLE profession("
        f"person_id MEDIUMINT({PERSON_ID_LEN}) UNSIGNED NOT NULL DEFAULT 0,"
        f"temp char({NCONST_LEN}) NOT NULL,"
        f"profession varchar({MAX_PROFESSION_LEN}) NOT NULL,"
        "PRIMARY KEY (id, profession),"
        "FOREIGN KEY (temp) REFERENCES person(temp)"
        ") ENGINE=InnoDB"
    )
    
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            print(err.msg)
            print(table_name)
