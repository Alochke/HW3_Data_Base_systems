import mysql.connector
from mysql.connector import errorcode

cnx = mysql.connector.connect(user = 'alonpolski', \
                              password = 'alon2285',\
                              host='127.0.0.1',\
                              database='alonpolski',\
                              port=3305
                            )

cursor = cnx.cursor()

if __name__ == "__main__":
  cnx.close()

