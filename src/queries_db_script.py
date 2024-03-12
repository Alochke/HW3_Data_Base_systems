import MySQLdb as mdb

con= mdb.connect('')

with con:
  cur= con.cursor(mdb.cursors.DictCursor)


#any param can be None
def query_1(show_adult, genre, start_range_year, end_range_year, start_range_runtime, end_range_runtime):
    flag=False #to mark if we need AND
    args=[]
    query= "SELECT title.name, title.year, title.minutes, title.ratings"
    if show_adult:
        query+= ", title.adult"
    query+=" FROM title"

    #specific genre
    if genre is not None:
        query+=" JOIN genre ON title.id = genre.title_id WHERE genre.genre = %s"
        args.append(genre)
        flag=True
    else:
        query+=" WHERE "

    #show adult films or not
    if not show_adult:
        if flag:
            query+=" AND"
        flag=True
        query+= " title.adult=0"
    
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

    #needs to add try catch...    print!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    cur.execute(query, args)


if __name__ == "__main__":
  cnx.close()