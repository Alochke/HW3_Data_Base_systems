import mysql.connector

cnx = mysql.connector.connect(user = 'alonpolski', \
                              password = 'alon2285',\
                              host='127.0.0.1',\
                              database='alonpolski',\
                              port=3305
                            )

cnx.close()