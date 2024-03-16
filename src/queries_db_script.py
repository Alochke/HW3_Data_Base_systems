
def query_1():
    """
    Returns a query that gets a genre and two year numbers,
    and returns a table that has movie genres, movie ids, movie names, the years in which production started, 
    duration in minutes and average imdb ratings (If no ratings were given the rating will be 0.) columns (in that order)
    that contains only the information of titles of the given genre that started the production between the given years.
    The result will be sorted by descending average rating.
    """

    query= (
        "SELECT genre.genre, title.id, title.name, title.year, title.minutes, title.ratings "
        "FROM genre JOIN title "
        "ON genre.id = title.id AND genre.genre LIKE %s AND title.year >= %s AND title.year <= %s "
        "ORDER BY title.ratings DESC"
        )

    return query


def query_2():
    """
    Returns a full-text query by movie name, and returns the matching rows in the title table.
    """   
    query = (
        "SELECT * "         
        "FROM title "
        "WHERE MATCH(name) AGAINST(%s IN NATURAL LANGUAGE MODE)"
        )
    return query


def query_3():
    """
    Returns a full-text query by movie name, that returns a table where the first column contains the ids of the movies,
    the second, their names,
    the third, a type of job in this movie's production,
    and the fourth, how many people did this job.
    If no data was gathered about a person doing a job on a production then there won't be a row for the film + job combination in the table.

    The result will be sorted by ascending movie_id.
    """
    query = (
            "SELECT A.id, A.name, title_person.job, COUNT(*) AS num_of_people "
            "FROM "
                "("
                    "SELECT id, name "
                    "FROM title "
                    "WHERE MATCH(name) AGAINST(%s IN NATURAL LANGUAGE MODE)"
                ") AS A JOIN title_person"
            "ON A.id = title_person.title_id "
            "GROUP BY A.id, title_person.job "
            "ORDER BY A.id"
            )

    return query

def query_4():
    """
    Returns a query that gets for a person the average rating of average imdb movie ratings where the person was part of the production,
    by the person's name.
    (Movies with no rating will be counted as having their average rating equal to zero.)

    The result will be sorted by descending average of averages.

    The query uses like to search by the person's name so we can use % to search for substrings.
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
    Returns a query that gets all industry professions having the amount of people working in them greater than the number given.
    """
    
    query = (
        "SELECT profession "
        "FROM profession JOIN person "
        "ON profession.id = person.id "
        "GROUP BY profession "
        "HAVING COUNT(*) > %s"
    )

    return query
