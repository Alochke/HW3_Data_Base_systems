import MySQLdb as mdb

con= mdb.connect('')

with con:
  cur= con.cursor(mdb.cursors.DictCursor)


new_view= "CREATE VIEW child_safe_movies AS SELECT title.id title.name, title.year, title.minutes, title.ratings FROM title WHERE title.adult = 0"




#search movie by multiple parameters
#any param can be None except show_adult
def query_1(name, show_adult, genre, start_range_year, end_range_year, start_range_runtime, end_range_runtime):
    flag=False #to mark if we need AND
    args=[]
    query= "SELECT title.id, title.name, title.year, title.minutes, title.ratings"
    
    if show_adult:
        query+= " ,title.adult"
    query+=" FROM title"

    #specific genre
    if genre is not None:
        query+=" JOIN genre ON title.id = genre.title_id WHERE genre.genre = %s"
        args.append(genre)
        flag=True
    else:
        query+=" WHERE "
    
    #filter adult films or not
    if not show_adult:
        if flag:
            query+=" AND"
        flag=True
        query+=" title.adult = 0"
    
    #search by movie name
    if name is not None:
        if flag:
            query+=" AND"
        flag=True
        query+=" MATCH(title.name) AGAINST('%s')"
        args.append(name)
    
    #year- in range
    if (start_range_year is not None) and (end_range_year is not None):
        if flag:
            query+=" AND"
        flag=True
        query+=" title.year IS NOT NULL"
    #from year
    if start_range_year is not None:
        if flag:
            query+=" AND"
        flag=True
        query+=" title.year >= %s"
        args.append(start_range_year)
    #to year
    if end_range_year is not None:
        if flag:
            query+=" AND"
        flag=True
        query+=" title.year <= %s"
        args.append(end_range_year)

    #runtime- in range
    if (start_range_runtime is not None) and (end_range_runtime is not None):
        if flag:
            query+=" AND"
        flag=True
        query+=" title.minutes IS NOT NULL"
    #from runtime
    if start_range_runtime is not None:
        if flag:
            query+=" AND"
        flag=True
        query+=" title.minutes >= %s"
        args.append(start_range_runtime)
    #to runtime
    if end_range_runtime is not None:
        if flag:
            query+=" AND"
        flag=True
        query+=" title.minutes <= %s"
        args.append(end_range_runtime)

    query+=" ORDER BY title.ratings DESC"

    #needs to add try catch?...    print!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    cur.execute(query, args)


#search by person: show the movies and the job of the person in the movie
#can filter by adult movies
def query_2(name, show_adult)
    query= "SELECT"
    if show_adult:
        query+= " title.adult,"
    
    query+=(" title.id AS movie_id, title.name AS movie_name, B.id AS person_id, B.name AS person_name, B.job"
            " FROM title JOIN"
                            " (SELECT person.id, person.name, title_person.job, title_person.film_id"
                             " FROM person JOIN title_person ON person.id = title_person.person_id")

    #search by person name
    if name is not None:
        query+=" WHERE MATCH(person.name) AGAINST('%s') ) AS B"
    else:
        query+=" ) AS B"

    query+=" ON title.id = B.film_id"
    
    #filter adult films or not
    if not show_adult:
        query+=" WHERE title.adult = 0"
            
    query+=" ORDER BY movie_id, person_id"

    #needs to add try catch?...    print!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if name is not None:
        cur.execute(query, name)
    else:
        cur.execute(query)




#search by person: show the movies and the job of the person in the movie
#can filter by adult movies
def query_2(name, show_adult)
    query= "SELECT"
    if show_adult:
        query+= " title.adult,"

    B=(" (SELECT person.id, person.name, title_person.job, title_person.film_id"
        " FROM person JOIN title_person ON person.id = title_person.person_id"
        " WHERE MATCH(person.name) AGAINST('%s') ) AS B")
    
    query+=(" title.id AS movie_id, title.name AS movie_name, B.id AS person_id, B.name AS person_name, B.job"
            " FROM title JOIN") + B +
            (" ON title.id = B.film_id")
    
    #filter adult films or not
    if not show_adult:
        query+=" WHERE title.adult = 0"
            
    query+=" ORDER BY movie_id, person_id"

    return query








#search by movie name and return table of name of movie, jobs in the movie and how many people are in this job ordered by movie name
#can filter by adult movies
def query_3(movie_name, show_adult):
    query= "SELECT"
    
    if show_adult:
        query+= " title.adult,"

    query+= (" title.id, title.name, title_person.job, COUNT(*) AS num_of_people"
            "FROM title JOIN title_person ON title.id = title_person.film_id")

    #search movie name
    if movie_name is not None:
        query+=" WHERE MATCH(title.name) AGAINST('%s')"

    #filter adult films or not
    if not show_adult:
        if movie_name is not None:
            query+=" AND title.adult = 0"
        else:
            query+=" WHERE title.adult = 0"

    query+=(" GROUP BY title.name, title_person.job"
            " ORDER BY title.name")

    #needs to add try catch?...    print!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if name is not None:
        cur.execute(query, movie_name)
    else:
        cur.execute(query)

    return query


#search by movie name and return table of name of movie, jobs in the movie and how many people are in this job ordered by movie name
#can filter by adult movies
def query_3(movie_name, show_adult):
    query= "SELECT"
    
    if show_adult:
        query+=" title.adult,"

    query+= (" title.id, title.name, title_person.job, COUNT(*) AS num_of_people"
            " FROM title JOIN title_person"
            " ON title.id = title_person.film_id"
            " WHERE MATCH(title.name) AGAINST('%s')")

    #filter adult films or not
    if not show_adult:
        query+=" AND title.adult = 0"

    query+=(" GROUP BY title.name, title_person.job"
            " ORDER BY title.name")

    return query










#view of average rating of all the people
new_view2= "CREATE VIEW avg_ratings AS SELECT A.id AS person_id, A.name, AVG(title.ratings) AS average_rating FROM"
    #A is a table of person_id, person_name and the film the person is in (in A a person is in a film no more then 1 time)
new_view2+=" (SELECT DISTINCT person.id, person.name, title_person.film_id"
new_view2+=" FROM person JOIN title_person ON person.id = title_person.person_id ) AS A"
new_view2+=" JOIN title ON A.film_id = title.id"
new_view2+=" GROUP BY A.id, A.name"



#search by person name and the average_rating
def query_4(name):
    query= "SELECT * FROM avg_ratings"
    #search by person name
    if name is not None:
        query+=" WHERE MATCH(avg_ratings.name) AGAINST('%s')"
    query+=" ORDER BY average_rating"

    #needs to add try catch?...    print!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if name is not None:
        cur.execute(query, name)
    else:
        cur.execute(query)




#search by person name: get the average_rating
def query_4(name):

    A=(" (SELECT DISTINCT person.id, person.name, title_person.film_id"
         " FROM person JOIN title_person"
         " ON person.id = title_person.person_id"
        " WHERE MATCH(person.name) AGAINST('%s') ) AS A")

    query= ("SELECT A.id, A.name, AVG(title.ratings) AS average_rating"
            " FROM title JOIN") + A +
            (" ON title.id = A.film_id"
            " GROUP BY A.id, A.name"
            " ORDER BY average_rating")

    return query


















#if profession is not None then return table of all the people in that profession ranked by their avg rating
#if is None then return table of professions, all the people in that profession ranked by their avg rating in the profession
def query_5(profession):
    if profession is not None:
        query="SELECT"
    else:
        query= "SELECT profession.profession,"
    
    query+=" avg_ratings.person_id, avg_ratings.name, avg_ratings.average_rating"
    
    if profession is not None:
        query+= ", DENSE_RANK() OVER ( ORDER BY avg_ratings.average_rating DESC ) rank"
    else:
        query+=", DENSE_RANK() OVER ( PARTITION BY profession.profession ORDER BY avg_ratings.average_rating DESC ) rank_in_profession"

    query+=" FROM profession JOIN avg_ratings ON profession.id = avg_ratings.person_id"
    
    if profession is not None:
        query+=" WHERE profession.profession = %s"

    #needs to add try catch?...    print!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if profession is not None:
        cur.execute(query, profession)
    else:
        cur.execute(query)


#table of all the people in the profession ranked by their avg rating
def query_5(profession):
    
    query=("SELECT avg_ratings.person_id, avg_ratings.name, avg_ratings.average_rating, DENSE_RANK() OVER ( ORDER BY avg_ratings.average_rating DESC ) rank"
            " FROM profession JOIN avg_ratings ON profession.id = avg_ratings.person_id"
            " WHERE profession.profession = %s")

    return query

    A=(" (SELECT profession.id"
        " FROM profession
        " WHERE profession.profession = %s)")
    
    B=(" (SELECT profession.id"
        " FROM person JOIN title_person"
        " ON person.id = title_person.person_id"
        " WHERE preson.id IN )")

    



















def query_6(movie_name, show_adult):
    
    query= "SELECT A.id AS movie_id, A.name AS movie_name"
    if show_adult:
        query+= " , A.adult AS is_adult_movie
    
    #filter adult films or not: A is the new movie table
    if show_adult:
        query+= " FROM title AS A"
    else:
        query+=" FROM child_safe_movies AS A"

    query+=(" JOIN (SELECT person.id, person.name, person.birthYear, person.deathYear, title_person.film_id, title_person.job"
            " FROM person JOIN title_person ON person.id = title_person.person_id ) AS B"
            " ON A.id = B.film_id")

    if movie_name is not None:
        query+=" WHERE MATCH(A.name) AGAINST('%s')"
        
    query+=" ORDER BY movie_id, person_id"

    if name is not None:
        cur.execute(query, movie_name)
    else:
        cur.execute(query)
