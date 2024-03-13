
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

#search by person name: show the movies and the job of the person in the movie
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


#search by movie name: return table of name of movie, jobs in the movie and how many people are in this job ordered by movie name
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



#by profession: table of all the people in the profession ranked by their avg rating
def query_5(profession):

    #list of IDs of people with that profession
    A=(" (SELECT profession.id"
        " FROM profession
        " WHERE profession.profession = %s)")
    
    #table of person id, name and the id of the movie the person is in
    B=(" (SELECT DISTINCT person.id, person.name, title_person.film_id"
        " FROM person JOIN title_person"
        " ON person.id = title_person.person_id"
        " WHERE preson.id IN") + A + (" ) AS B")
    
    #table of person id, name and all the ratings of the person films
    C=(" (SELECT B.id, B.name, title.ratings"
        " FROM title JOIN") + B +
        (" ON title.id = B.film_id) AS C")

    query=("SELECT C.id, C.name, AVG(C.ratings) AS average_rating, DENSE_RANK() OVER ( ORDER BY average_rating DESC ) rank"
            " FROM") + C +
            (" GROUP BY C.id, C.name")

    return query


#search by movie name: return table of name of movie id and name, jobs in the movie and the id and name of the person with the job
def query_6(movie_name):

    #table of id, name of films with a name like the parameter
    A=(" (SELECT title.id, title.name"
        "FROM title"
        "WHERE MATCH(title.name) AGAINST('%s') ) AS A")
    
    query= ("SELECT A.id AS film_id ,A.name AS film_name, person.id AS person_id, person.name AS person_name, title_person.job"
            " FROM") + A + 
            (" JOIN title_person ON A.id = title_person.film_id"
            " JOIN person ON title_person.person_id = person.id")

    return query
