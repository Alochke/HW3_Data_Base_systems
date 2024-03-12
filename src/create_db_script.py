import mysql

MOVIE_ID_LEN = 10
PERSON_ID_LEN = 10
MOVIE_LEN = 5
MAX_MOVIE_NAME_LEN = 208
MAX_MINUTES_LEN = 5
MAX_GENRE_LEN = 11 
MAX_JOB_LEN = 19
MAX_PERSON_NAME_LEN = 95
MAX_PROFESSION_LEN = 25

def create_tables(cursor: mysql.connector.cursor_cext.CMySQLCursor):
    TABLES = {}
    TABLES['title'] = (
        "CREATE TABLE title("
        f"id char({MOVIE_ID_LEN}) NOT NULL,"
        f"type char({MOVIE_LEN}) NOT NULL,"
        f"name varchar({MAX_MOVIE_NAME_LEN}) NOT NULL,"
        "adult BOOL NOT NULL,"
        "year YEAR NOT NULL,"
        f"minutes SMALLINT({MAX_MINUTES_LEN}) UNSIGNED NOT NULL,"
        "ratings FLOAT(2) NOT NULL,"
        "PRIMARY KEY (id),"
        "CONSTRAINT type_check CHECK (type LIKE 'movie')"
        ") ENGINE=InnoDB"
    )
    TABLES['genre'] = (
        "CREATE TABLE genre("
        f"id char({MOVIE_ID_LEN}) NOT NULL,"
        f"genre varchar({MAX_GENRE_LEN}) NOT NULL,"
        "PRIMARY KEY (id, genre),"
        "FOREIGN KEY (id) REFERENCES title(id)"
        ") ENGINE=InnoDB"
    )
    TABLES['title_person'] = (
        "CREATE TABLE title_person("
        f"title_id char({MOVIE_ID_LEN}) NOT NULL,"
        f"person_id char({PERSON_ID_LEN}) NOT NULL,"
        f"job varchar({MAX_JOB_LEN}) NOT NULL,"
        "PRIMARY KEY (title_id, person_id, job),"
        "FOREIGN KEY (title_id) REFERENCES title(id)"
        ") ENGINE=InnoDB"
    )
    TABLES['person'] = (
        "CREATE TABLE person("
        f"id char({PERSON_ID_LEN}) NOT NULL,"
        f"name varchar({MAX_PERSON_NAME_LEN}) NOT NULL,"
        "FOREIGN KEY (id) REFERENCES title_person(person_id),"
        "PRIMARY KEY (id)"
        ") ENGINE=InnoDB"
    )
    TABLES['profession'] = (
        "CREATE TABLE profession("
        f"id char({PERSON_ID_LEN}) NOT NULL,"
        f"profession varchar({MAX_PROFESSION_LEN}) NOT NULL,"
        "PRIMARY KEY (id, profession),"
        "FOREIGN KEY (id) REFERENCES person(id)"
        ") ENGINE=InnoDB"
    )
    
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            print(err.msg)
