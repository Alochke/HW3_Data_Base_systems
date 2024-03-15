
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
    Retutns a sql string that searches by a person's name movies where the person was part of production,
    and a job type he did on this production.

    The table will have five columns,
    the first contains the id of a person,
    the second his/her name,
    the third is an id of a movie where the person was part of production,
    the fourth is the name of the movie,
    and the fifth is the type of job the person did on that movie.

    The result will be sorted by ascending person_id first, and will have an inner order dictated by ascending movie_id.
    """    
    query =(
            "SELECT B.id AS person_id, B.name AS person_name, title.id AS movie_id, title.name AS movie_name, B.job "
            "FROM title JOIN "
                "("
                    "SELECT DISTINCT A.id, A.name, title_person.title_id "
                    "FROM title_person JOIN "
                        "("
                            "SELECT person.id, person.name "
                            "FROM person "
                            "WHERE MATCH(person.name) AGAINST('%s' IN NATURAL LANGUAGE MODE)"
                        ") AS A "
                    "ON title_person.person_id = A.id"
                ") AS B "
            "ON title.id = B.title_id "
            "ORDER BY person_id, movie_id"
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
            # One could say that we have code duplication here because below there's exactly the same substring we have in query number 3,
            # however I think this code duplication justifies itself because that make the code more readable.
            "FROM title JOIN "
                "("
                    "SELECT DISTINCT A.id, A.name, title_person.title_id "
                    "FROM title_person JOIN "
                        "("
                            "SELECT person.id, person.name "
                            "FROM person "
                            "WHERE MATCH(person.name) AGAINST('%s' IN NATURAL LANGUAGE MODE)"
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
