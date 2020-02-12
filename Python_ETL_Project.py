import sqlite3
import sys

#Drop table sql
def drp_tbl(table):
    return 'DROP TABLE IF EXISTS ' + table

#Split incoming Movie data to Normalize the data
def split_mov_data(line):
    attr_list = line.split(sep='::')                                   #'::' are used as separator in the file
    if len(attr_list) != 3:                                            #length of 3 attributes are expected from input file
       return 
    else:
       year = attr_list[1].strip()[attr_list[1].rfind('(')+1:-1]       #finds rightmost '(' and returns str following that pos
       title = attr_list[1].strip()[:attr_list[1].rfind('(')].strip()  #finds rightmost '(' and returns str before that pos
       cat = attr_list[2].strip().split(sep='|')                       #Movie genre is separated by '|'; this will return list
       mov_id = attr_list[0]
       return mov_id, year, title, cat

#Queries table for record counts
def sel_qry(table):
    return 'SELECT count(*) FROM ' + table

#Procedure for Age Decode table
def age_tbl(cursor, data_in):
    print('Initiation Age ref table Procedure')
    
    #Drop Table
    print('Dropping \'age_decode\' table')
    try:
        cursor.execute(drp_tbl('age_decode'))
    except:
        print('Failed to drop \'age_decode\' table')
        return False

    #CREATE Age TABLE
    crtsql_age = '''CREATE TABLE age_decode (age_code    VARCHAR2(2) CONSTRAINT age_pk PRIMARY KEY NOT NULL,
                                             age_group   VARCHAR2(10))'''
    
    print('Creating \'age_decode\' table')
    try:
        cursor.execute(crtsql_age)                 
        print('Created \'age_decode\' table')
    except:
        print('Failed to create \'age_decode\' table')
        return False

    #Insert Age table sql                                         
    insql_age = '''INSERT OR IGNORE INTO age_decode VALUES(?,?)'''
    
    print('Loading data into \'age_decode\' table...')
    try:
        cursor.executemany(insql_age, data_in)
        RecordsProcessed = len(data_in)
        print('Processed ' + str(RecordsProcessed) + ' records')        
        print('Loading complete')
    except:
        print('Failed to load data into \'age_decode\' table')
        return False
    return True

#Procedure for Occupation table
def occupation_tbl(cursor, data_in):
    print('Initiation \'occupation_decode\' table Procedure')
    
    #Drop Table
    print('Dropping \'occupation_decode\' table')
    try:
        cursor.execute(drp_tbl('occupation_decode'))
    except:
        print('Failed to drop \'occupation_decode\' table')
        return False

    #CREATE TABLE
    crtsql_occupation = '''CREATE TABLE occupation_decode (occupation_code  VARCHAR2(2) CONSTRAINT occu_pk PRIMARY KEY NOT NULL,
                                                           occupation_group VARCHAR2(35))'''
    
    print('Creating \'occupation_decode\' table')
    try:
        cursor.execute(crtsql_occupation)                 
        print('Created \'occupation_decode\' table')
    except:
        print('Failed to create \'occupation_decode\' table')
        return False

    #Insert Occupation table sql                                         
    insql_occupation = '''INSERT OR IGNORE INTO occupation_decode VALUES(?,?)'''
    
    print('Loading data into \'occupation_decode\' table...')
    try:
        cursor.executemany(insql_occupation, data_in)
        RecordsProcessed = len(data_in)
        print('Processed ' + str(RecordsProcessed) + ' records')
        print('Loading complete')
    except:
        print('Failed to load data into \'occupation_decode\' table')
        return False
    return True

#Procedure for Users table
def usr_tbl(cursor, data_in):
    print('Initiation Users table Procedure')
    RecordsProcessed = 0                                    #Total records processed counter; including counts of bad records
    
    #Drop Table
    print('Dropping \'users\' table')
    try:
        cursor.execute(drp_tbl('users'))
    except:
        print('Failed to drop \'users\' table')
        return False

    #CREATE TABLE
    crtsql_users = '''CREATE TABLE users (user_id     VARCHAR2(4) CONSTRAINT usr_pk PRIMARY KEY NOT NULL,
                                          gender      CHAR(1),
                                          age         VARCHAR2(2)
                                            CONSTRAINT usr_FK_1
                                                REFERENCES age_decode(age_code),
                                          occupation  VARCHAR2(2)
                                            CONSTRAINT usr_FK_2
                                                REFERENCES occupation_decode(occupation_code),
                                          ZIP         VARCHAR2(10))'''
    
    print('Creating \'users\' table')
    try:
        cursor.execute(crtsql_users)                 
        print('Created \'users\' table')
    except:
        print('Failed to create \'users\' table')
        return False

    #Insert Users table sql                                         
    insql_users = '''INSERT OR IGNORE INTO users VALUES(?,?,?,?,?)'''
    
    print('Opening ' + data_in + ' file')
    try:
        file_in = open(data_in,'r')                                    #Opening input file as read-only
    except:
        print(data_in + ' file not found')
        return False
    
    print('Loading data into \'users\' table...')
    read_line = file_in.readline()                                     #Reading single line at a time
    next_line = True                                                   #True while there's more data to process
    #While there's more data to process, split the line into attributes and then load into table
    while next_line:
        attr_list = read_line.split('::')
        RecordsProcessed += 1
        if len(attr_list) == 5:                                        #Users table expects input of 5 attributes
           cursor.execute(insql_users, attr_list)
        else:
           print('Skipping bad record')
           #Check for more records
           read_line = file_in.readline()
           if len(read_line) < 1:
              next_line = False
           else:
              next_line = True
           continue
        
        #Check for more records
        read_line = file_in.readline()
        if len(read_line) < 1:
           next_line = False
        else:
           next_line = True
    file_in.close()
    print('Loading complete')
    print('Processed ' + str(RecordsProcessed) + ' records')
    return True

#Procedure for Users table
def movie_tbls(cursor, data_in):
    print('Initiating Movies and Category tables procedures')
    RecordsProcessed = 0                                    #Total records processed counter; including counts of bad records
    
    #Drop Movies Table
    print('Dropping \'movies\' table')
    try:
        cursor.execute(drp_tbl('movies'))
    except:
        print('Failed to drop \'movies\' table')
        return False

    #Drop Category Table
    print('Dropping \'movie_cat\' table')
    try:
        cursor.execute(drp_tbl('movie_cat'))
    except:
        print('Failed to drop \'movie_cat\' table')
        return False
        
    #CREATE TABLE    
    crtsql_movies = '''CREATE TABLE movies (movie_id    VARCHAR2(4) CONSTRAINT mov_pk PRIMARY KEY NOT NULL,
                                            title       VARCHAR2(100),
                                            movie_year  VARCHAR2(4))'''

    crtsql_cat = '''CREATE TABLE movie_cat (movie_id    VARCHAR2(4),
                                            movie_cat   VARCHAR2(47),
                                                CONSTRAINT cat_pk PRIMARY KEY (movie_id, movie_cat))'''

    print('Creating \'movies\' table')
    try:
        cursor.execute(crtsql_movies)                 
        print('Created \'movies\' table')
    except:
        print('Failed to create \'movies\' table')
        return False
    
    print('Creating \'movie_cat\' table')
    try:
        cursor.execute(crtsql_cat)
        print('Created \'movie_cat\' table')
    except:
        print('Failed to create \'movie_cat\' table')
        return False

    #Movies and Movie_Cat tables' INSERT sql
    insql_movies = '''INSERT OR IGNORE INTO movies VALUES(?,?,?)'''
    insql_cat = '''INSERT OR IGNORE INTO movie_cat VALUES(?,?)'''

    print('Opening ' + data_in + ' file')
    try:
        file_in = open(data_in,'r')                                    #Opening input file as read-only
    except:
        print(data_in + ' file not found')
        return False

    print('Loading data into \'movies\' and \'movie_cat\' tables...')
    read_line = file_in.readline()                                     #Reading single line at a time
    next_line = True                                                   #True while there's more data to process    
    #While there's more data to process, split the line into attributes and then load into table
    while next_line:
        RecordsProcessed += 1
        try:
            movie_id, year, title, category = split_mov_data(read_line)
        except:
            print('Skipping bad record...')
            #Check for more records
            read_line = file_in.readline()
            if len(read_line) < 1:
               next_line = False
            else:
               next_line = True
            continue
        
        cursor.execute(insql_movies, (movie_id, title, year))          #loads movies table
        for cat in category:                                           #loads category table; cat is a list hence need to loop
            cursor.execute(insql_cat, (movie_id, cat))
            
        #Check for more records
        read_line = file_in.readline()
        if len(read_line) < 1:
           next_line = False
        else:
           next_line = True
    file_in.close()
    print('Loading complete')
    print('Processed ' + str(RecordsProcessed) + ' records')
    return True

#Procedure for Users table
def rating_tbl(cursor, data_in):
    print('Initiating Rating table Procedure')
    RecordsProcessed = 0                                   #Total records processed counter; including counts of bad records
    
    #Variables to execute sql in batches
    RatBatchRows = []                                      #To store list of tuples to be inserted into table using executemany
    RatBatchCnt = 25000                                    #Runs executemany method once tuple list length equals batch count
    RatBatchRowCt = 0                                      #List append counter
    
    #Drop Table
    print('Dropping \'ratings\' table')
    try:
        cursor.execute(drp_tbl('ratings'))
    except:
        print('Failed to drop \'ratings\' table')
        return False

    #CREATE TABLE
    crtsql_ratings = '''CREATE TABLE ratings (user_id   VARCHAR2(4)
                                                CONSTRAINT rat_FK_1
                                                    REFERENCES users(user_id),
                                              movie_id  VARCHAR2(4)
                                                CONSTRAINT rat_FK_2
                                                    REFERENCES movies(movie_id),
                                              rating    CHAR(1),
                                              tmst      VARCHAR2(10),
                                                CONSTRAINT rat_PK PRIMARY KEY (user_id, movie_id))'''
                                                
    print('Creating \'ratings\' table')
    try:
        cursor.execute(crtsql_ratings)                 
        print('Created \'ratings\' table')
    except:
        print('Failed to create \'ratings\' table')
        return False

    #Insert ratings table sql
    insql_ratings = '''INSERT OR IGNORE INTO ratings VALUES(?,?,?,?)'''
    
    print('Opening ' + data_in + ' file')
    try:
        file_in = open(data_in,'r')                                    #Opening input file as read-only
    except:
        print(data_in + ' file not found')
        return False
    
    print('Loading data into \'ratings\' table...')
    read_line = file_in.readline()                                     #Reading single line at a time
    next_line = True                                                   #True while there's more data to process
    #While there's more data to process, split the line into attributes and then load into table
    while next_line:
        RecordsProcessed += 1
        attr_list = read_line.split('::')
        if len(attr_list) == 4:                                 #Ratings table expects input of exactly 4 attributes
           RatBatchRows.append(attr_list)                       #Append to the batch list; executes when equals max batch count
           RatBatchRowCt += 1
        else:
           print('Skipping bad record')
           #Check for more records
           read_line = file_in.readline()
           if len(read_line) < 1:
              next_line = False
              cursor.executemany(insql_ratings, RatBatchRows)
              RatBatchRows = []
              RatBatchRowCt = 0
           else:
              next_line = True
           continue
        
        if RatBatchRowCt == RatBatchCnt:
           cursor.executemany(insql_ratings, RatBatchRows)
           RatBatchRows = []
           RatBatchRowCt = 0
           print('Processed ' + str(RecordsProcessed) + ' records so far...')
        else:
           #Check for more records
           read_line = file_in.readline()
           if len(read_line) < 1:
              next_line = False
              cursor.executemany(insql_ratings, RatBatchRows)
              RatBatchRows = []
              RatBatchRowCt = 0
           else:
              next_line = True
    file_in.close()
    print('Processed ' + str(RecordsProcessed) + ' records')
    print('Loading complete')
    return True
    
#---------------------------------------------------------------------------------------------

#Declaring Variables    
#Input files
movie_file = 'movies.dat'
users_file = 'users.dat'
ratings_file = 'ratings.dat'

#Data for age_decode table
age_data = [['1', 'Under 18']
          , ['18', '18-24']
          , ['25', '25-34']
          , ['35', '35-44']
          , ['45', '45-49']
          , ['50', '50-55']
          , ['56', '56+']]
#Data for occupation_decode table
occpn_data = [['0','other']
            , ['1','academic/educator']
            , ['2','artist']
            , ['3','clerical/admin']
            , ['4','college/grad student']
            , ['5','customer service']
            , ['6','doctor/health care']
            , ['7','executive/managerial']
            , ['8','farmer']
            , ['9','homemaker']
            , ['10','K-12 student']
            , ['11','lawyer']
            , ['12','programmer']
            , ['13','retired']
            , ['14','sales/marketing']
            , ['15','scientist']
            , ['16','self-employed']
            , ['17','technician/engineer']
            , ['18','tradesman/craftsman']
            , ['19','unemployed']
            , ['20','writer']]               


#Creates Connection obj rep DB
#If DB exists, obj creates connection to DB or else creates new DB of given nm
conn = sqlite3.connect('CSC452_HW6.sqlite')    
cur = conn.cursor()                                                    #Ceate Cursor obj in use its methods like, execute()

#Pass connection cursor and data as param
#Calling Age Procedure
print('Calling Age Ref Table Procedure')
if age_tbl(cur, age_data):
    conn.commit()
    #Check number of records loaded
    rec_cnt = cur.execute(sel_qry('age_decode')).fetchall()
    print('Successfully loaded ' + str(rec_cnt[0][0]) + ' records to \'age_decode\' table\n')
else:
    conn.rollback()
    cur.close()                     
    conn.close()                    
    print('Error executing Age Ref table procedure')
    print('Rolled back transactions since last commit and closed DB Connection\n')
    sys.exit()

#Calling Occupation Procedure
print('Calling Occupation Ref Table Procedure')
if occupation_tbl(cur, occpn_data):
    conn.commit()
    #Check number of records loaded
    rec_cnt = cur.execute(sel_qry('occupation_decode')).fetchall()
    print('Successfully loaded ' + str(rec_cnt[0][0]) + ' records to \'occupation_decode\' table\n')
else:
    conn.rollback()
    cur.close()                     
    conn.close()                    
    print('Error executing Age Ref table procedure')
    print('Rolled back transactions since last commit and closed DB Connection\n')
    sys.exit()

#Calling Users Procedure    
print('Calling Users Table Procedure')
if usr_tbl(cur, users_file):
    conn.commit()
    #Check number of records loaded
    rec_cnt = cur.execute(sel_qry('users')).fetchall()
    print('Successfully loaded ' + str(rec_cnt[0][0]) + ' records to \'users\' table\n')
else:
    conn.rollback()
    cur.close()                     
    conn.close()                    
    print('Error executing Age Ref table procedure')
    print('Rolled back transactions since last commit and closed DB Connection\n')
    sys.exit()

#Calling Movies Procedure    
print('Calling Movies Table Procedure')
if movie_tbls(cur, movie_file):
    conn.commit()
    #Check number of records loaded
    rec_cnt = cur.execute(sel_qry('movies')).fetchall()
    print('Successfully loaded ' + str(rec_cnt[0][0]) + ' records to \'movies\' table')
    rec_cnt = cur.execute(sel_qry('movie_cat')).fetchall()
    print('Successfully loaded ' + str(rec_cnt[0][0]) + ' records to \'movie_cat\' table\n')
else:
    conn.rollback()
    cur.close()                     
    conn.close()                    
    print('Error executing Age Ref table procedure')
    print('Rolled back transactions since last commit and closed DB Connection\n')
    sys.exit()

#Calling Ratings Procedure    
print('Calling Ratings Table Procedure')
if rating_tbl(cur, ratings_file):
    conn.commit()
    #Check number of records loaded
    rec_cnt = cur.execute(sel_qry('ratings')).fetchall()
    print('Successfully loaded ' + str(rec_cnt[0][0]) + ' records to \'ratings\' table\n')
else:
    conn.rollback()
    cur.close()                     
    conn.close()                    
    print('Error executing Age Ref table procedure')
    print('Rolled back transactions since last commit and closed DB Connection\n')
    sys.exit()

#Analysis queries
print('Running Analysis Queries\n')

#Below query identifies gender of users with ratings data
#Based on the output of the below query, only 25% of the ratings were by Females
qry_1 = '''SELECT a.gender, count(c.rating) rating_cnts
           FROM users a
           INNER JOIN age_decode b
           ON (a.age = b.age_code)
           LEFT JOIN ratings c
           ON (a.user_id = c.user_id)
           GROUP BY a.gender
           ORDER BY rating_cnts DESC
           ;'''
qry_rslt = cur.execute(qry_1).fetchall()
print('Movie rating counts by gender')
print('Gender\tRating_Counts')
for rw in qry_rslt:
    print(str(rw[0]) + '\t' + str(rw[1]))
print('\n')

#Below query identifies movie categories highly rated by male respondants 
#Drama, Comedy, and Action ranked highly
qry_2 = '''SELECT b.movie_cat, count(c.user_id) user_cnt
           FROM ratings a
           INNER JOIN movie_cat b
           ON (a.movie_id = b.movie_id)
           INNER JOIN users c
           ON (a.user_id = c.user_id)
           WHERE c.gender = 'M'
             AND a.rating > 3
           GROUP BY b.movie_cat
           ORDER BY user_cnt DESC;'''
           
qry_rslt = cur.execute(qry_2).fetchall()
print('Highly rated movie categories by Male respondants')
print('Category\tUser_Counts')
for rw in qry_rslt:
    print(str(rw[0]) + '\t' + str(rw[1]))
print('\n')           
           

#Below query identifies movie categories highly rated by male respondants 
#Drama, Comedy, and Romance ranked highly
qry_3 = '''SELECT b.movie_cat, count(c.user_id) user_cnt
           FROM ratings a
           INNER JOIN movie_cat b
           ON (a.movie_id = b.movie_id)
           INNER JOIN users c
           ON (a.user_id = c.user_id)
           WHERE c.gender = 'F'
             AND a.rating > 3
           GROUP BY b.movie_cat
           ORDER BY user_cnt DESC;'''

qry_rslt = cur.execute(qry_3).fetchall()
print('Highly rated movie categories by Female respondants')
print('Category\tUser_Counts')
for rw in qry_rslt:
    print(str(rw[0]) + '\t' + str(rw[1]))
print('\n')

print('End of script')
    
cur.close()                     #Close cursor
conn.close()                    #Close DB connection
