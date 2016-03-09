
#!/usr/bin/python

import sys
import mysql.connector
from mysql.connector import errorcode

if len(sys.argv) != 3:
    print "Usage: CSVtoMySQL.py file.txt tablename"
    sys.exit()

inputfile = sys.argv[1]
tablename = sys.argv[2]

fo = open(inputfile, 'r')
lines = fo.read().split('\n')

# add every first comma-delimited item to the set, except skip the first and last lines
#for line in lines[1:len(lines) - 1:]:
#    primarykeys.add(line.split(',')[0])

#mysql setup
try:
    con = mysql.connector.connect(user='hackathon',
                                password='mississauga',
                                host='localhost',
                                database='codeinthecity')
    cursor = con.cursor()
    inputformat = ''
    limit = len(lines[0].split(','))
    for i in range(limit):
        if i < limit - 1:
            inputformat += '%s,'
        else:
            inputformat += '%s'

    insert = "INSERT INTO " + tablename + " VALUES (" + inputformat + ")";

    for line in lines[1:len(lines):]:
        if len(line) != 0:
            print(line)
            cursor.execute(insert, line.split(','))

    con.commit()
    con.close()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    con.close()
