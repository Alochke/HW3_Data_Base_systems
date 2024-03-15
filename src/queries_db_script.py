
# search movie by multiple parameters
def query_1():
    """
    Retruns a query that gets a genre and two year numbers,
    and returns a table that has movie genres, movie ids, movie name, years, duration in minutes and average imdb ratings columns (in that order)
    that contains only the information of titles of the given genre that started the production between the given years.
    The result will be sorted by descending average rating.
    """

    query= (
        "SELECT genre.genre, title.id, title.name, title.year, title.minutes, title.ratings "
        "FROM genre JOIN title "
        "ON MATCH(genre.genre) AGAINST(%s IN NATURAL LANGUAGE MODE) AND title.year >= %s AND title.year <= %s "
        "ORDER BY title.ratings DESC"
        )

    return query


def query_2():
    """
    Retutns a query that searches by movie name, and returns their row in the title table
    """   
    query = (
        "SELECT * "         
        "FROM title "
        "WHERE MATCH(name) AGAINST(%s IN NATURAL LANGUAGE MODE)"
        )
    return query


def query_3():
    """
    Returns a query that searches by movie name, and returns a table where the first column contains the ids of the movies,
    the second their names,
    a type of job in this movie
    and how many people did this job.

    The result will be sorted by ascending person_id movie_id.
    """
    query = (
            "SELECT A.id, A.name, title_person.job, COUNT(*) AS num_of_people "
            "FROM "
                "("
                    "SELECT id, name "
                    "FROM title "
                    "WHERE MATCH(name) AGAINST('%s' IN NATURAL LANGUAGE MODE)"
                ") AS A JOIN title_person "
            "ON A.id = title_person.title_id "
            "GROUP BY A.id, title_person.job "
            "ORDER BY A.id"
            )

    return query

def query_4():
    """
    Get for a person the average rating of average imdb movie ratings where the person was part of the production, by the person's name.

    The result will be sorted by descending average of averages.
    """
    query =(
            "SELECT B.id, B.name, AVG(title.ratings) AS average_rating "
            "FROM title JOIN "
                "("
                    "SELECT DISTINCT A.id, A.name, title_person.title_id "
                    "FROM title_person JOIN "
                        "("
                            "SELECT person.id, person.name "
                            "FROM person "
                            "WHERE person.name like %s"
                        ") AS A "
                    "ON title_person.person_id = A.id"
                ") AS B "
            "ON title.id = B.title_id "
            "GROUP BY B.id, B.name "
            "ORDER BY average_rating DESC"
            )

    return query

def query_5():
    """
    Returns a query that gets all industry professions having amount of people working in them greater than the number given.
    """
    
    query = (
        "SELECT profession"
        "FROM profession JOIN person"
        "ON profession.id = person.id"
        "GROUP BY proffesion"
        "HAVING COUNT(*) > %d"
    )

    return query
