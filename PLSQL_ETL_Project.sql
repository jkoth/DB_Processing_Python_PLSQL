--Procedure for creating ratings table    
CREATE OR REPLACE PROCEDURE crt_rat_tbl IS
    drpsql  VARCHAR2(1000):='DROP TABLE ratings CASCADE CONSTRAINTS PURGE';
    crtsql  VARCHAR2(1000):='CREATE TABLE ratings (user_id     VARCHAR2(4)
                                                        CONSTRAINT rat_FK_1
                                                        REFERENCES users(user_id),
                                                   movie_id    VARCHAR2(4)
                                                        CONSTRAINT rat_FK_2
                                                        REFERENCES movies(movie_id),
                                                   rating      CHAR(1),
                                                   tmst        VARCHAR2(10),
                                                   CONSTRAINT rat_PK PRIMARY KEY (user_id, movie_id))';
BEGIN
  DBMS_OUTPUT.PUT_LINE('Dropping Ratings Table');
  EXECUTE IMMEDIATE drpsql;
  DBMS_OUTPUT.PUT_LINE('Creating Ratings Table');
  EXECUTE IMMEDIATE crtsql;
  DBMS_OUTPUT.PUT_LINE('Created Ratings Table');  
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Nothing to drop');
      DBMS_OUTPUT.PUT_LINE('Creating Ratings Table');
      EXECUTE IMMEDIATE crtsql;
      DBMS_OUTPUT.PUT_LINE('Created Ratings Table');  
END crt_rat_tbl;
/

--Procedure for loading ratings table    
CREATE OR REPLACE PROCEDURE insert_rat_tbl IS
    CURSOR in_rat_tbl IS
        SELECT *
        FROM RATING_RAW;
    inssql      VARCHAR2(1000):='INSERT INTO ratings VALUES(:a,:b,:c,:d)';
    row_count   BINARY_INTEGER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Loading ratings Table');
    FOR rw IN in_rat_tbl LOOP
        EXECUTE IMMEDIATE inssql
        USING rw.COL_1,rw.COL_2,rw.COL_3,rw.COL_4;
        row_count := in_rat_tbl%ROWCOUNT;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(row_count || ' records loaded to Ratings table');
END insert_rat_tbl;
/

--Procedure for creating users table
CREATE OR REPLACE PROCEDURE crt_usr_tbl IS
    drpsql  VARCHAR2(1000):='DROP TABLE users CASCADE CONSTRAINTS PURGE';
    crtsql  VARCHAR2(1000):='CREATE TABLE users (user_id     VARCHAR2(4) CONSTRAINT usr_pk PRIMARY KEY NOT NULL,
                                                 gender      CHAR(1),
                                                 age         VARCHAR2(2)
                                                        CONSTRAINT usr_FK_1
                                                        REFERENCES age_decode(age_code),
                                                 occupation  VARCHAR2(2)
                                                        CONSTRAINT usr_FK_2
                                                        REFERENCES occupation_decode(occupation_code),
                                                 ZIP         VARCHAR2(10))';
BEGIN
  DBMS_OUTPUT.PUT_LINE('Dropping Users Table');
  EXECUTE IMMEDIATE drpsql;
  DBMS_OUTPUT.PUT_LINE('Creating Users Table');
  EXECUTE IMMEDIATE crtsql;
  DBMS_OUTPUT.PUT_LINE('Created Users Table');  
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Nothing to drop');
      DBMS_OUTPUT.PUT_LINE('Creating Users Table');
      EXECUTE IMMEDIATE crtsql;
      DBMS_OUTPUT.PUT_LINE('Created Users Table');  
END crt_usr_tbl;
/

--Procedure for loading users table    
CREATE OR REPLACE PROCEDURE insert_usr_tbl IS
    CURSOR in_usr_tbl IS
        SELECT *
        FROM USERS_RAW;
    inssql  VARCHAR2(1000):='INSERT INTO users VALUES(:a,:b,:c,:d,:e)';
    row_count   BINARY_INTEGER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Loading Users Table');
    FOR rw IN in_usr_tbl LOOP
        EXECUTE IMMEDIATE inssql
        USING rw.COLUMN1,rw.COLUMN2,rw.COLUMN3,rw.COLUMN4,rw.COLUMN5;
        row_count := in_usr_tbl%ROWCOUNT;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(row_count || ' records loaded to Users table');
END insert_usr_tbl;
/

--Procedure for creating movies table
CREATE OR REPLACE PROCEDURE crt_movie_tbl IS
    drpsql  VARCHAR2(1000):='DROP TABLE movies CASCADE CONSTRAINTS PURGE';
    crtsql  VARCHAR2(1000):='CREATE TABLE movies (movie_id    VARCHAR2(4) CONSTRAINT mov_pk PRIMARY KEY NOT NULL,
                                                  title       VARCHAR2(100),
                                                  movie_year  VARCHAR2(4))';
BEGIN
  DBMS_OUTPUT.PUT_LINE('Dropping Movies Table');
  EXECUTE IMMEDIATE drpsql;
  DBMS_OUTPUT.PUT_LINE('Creating Movies Table');
  EXECUTE IMMEDIATE crtsql;
  DBMS_OUTPUT.PUT_LINE('Created Movies Table');  
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Nothing to drop');
      DBMS_OUTPUT.PUT_LINE('Creating Movies Table');
      EXECUTE IMMEDIATE crtsql;
      DBMS_OUTPUT.PUT_LINE('Created Movies Table');  
END crt_movie_tbl;
/

--Procedure for loading movies table    
CREATE OR REPLACE PROCEDURE insert_movie_tbl IS
    CURSOR in_mov_tbl IS
        SELECT COLUMN1, COLUMN2
        FROM MOVIES_RAW;
    title   VARCHAR2(100);
    yr      VARCHAR2(4);
    inssql  VARCHAR2(1000):='INSERT INTO movies VALUES(:a, :b, :c)';
    row_count   BINARY_INTEGER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Loading Movies Table');
    FOR rw IN in_mov_tbl LOOP
        title := trim(replace(trim(rw.column2),substr(trim(rw.column2),(instr(rw.column2,'(',-1)),6),''));
        yr := substr(trim(rw.column2), (instr(trim(rw.column2),'(',-1))+1,4);
        EXECUTE IMMEDIATE inssql
        USING rw.column1, title, yr;
        row_count := in_mov_tbl%ROWCOUNT;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(row_count || ' records loaded to Movies table');
END insert_movie_tbl;
/

--Procedure for creating movies-category table
CREATE OR REPLACE PROCEDURE crt_cat_tbl IS
    drpsql  VARCHAR2(1000):='DROP TABLE movie_cat CASCADE CONSTRAINTS PURGE';
    crtsql  VARCHAR2(1000):='CREATE TABLE movie_cat (movie_id    VARCHAR2(4),
                                                     movie_cat   VARCHAR2(47),
                                                     CONSTRAINT cat_pk PRIMARY KEY (movie_id, movie_cat))';
BEGIN
  DBMS_OUTPUT.PUT_LINE('Dropping Movie-Category Table');
  EXECUTE IMMEDIATE drpsql;
  DBMS_OUTPUT.PUT_LINE('Creating Movie-Category Table');
  EXECUTE IMMEDIATE crtsql;
  DBMS_OUTPUT.PUT_LINE('Created Movie-Category Table');
  EXCEPTION                                     
    WHEN OTHERS THEN                                --avoids error when running procedure without existing table
      DBMS_OUTPUT.PUT_LINE('Nothing to drop');            
      DBMS_OUTPUT.PUT_LINE('Creating Movie-Category Table');
      EXECUTE IMMEDIATE crtsql;
      DBMS_OUTPUT.PUT_LINE('Created Movie-Category Table');
END crt_cat_tbl;
/

--Procedure for loading movie-category table    
CREATE OR REPLACE PROCEDURE insert_cat_tbl IS    
    --Declaring Cursor which reads all records from raw movie input in order to create another able with 
    CURSOR split_genre IS
        SELECT COLUMN1, COLUMN3
        FROM MOVIES_RAW;
    cat_count BINARY_INTEGER;                       --Used to count number of pipes in a given value
    cat_val VARCHAR2(47);                           --Used to capture individual Genre/Category from multiple piped categories
    inssql  VARCHAR2(1000):='INSERT INTO movie_cat VALUES(:a, :b)';
    row_count   BINARY_INTEGER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Loading Movie-Category Table');
    FOR rw IN split_genre LOOP
        cat_count := regexp_count(rw.column3,'\|',1);               --Counts the number of times given pattern exists in value
        IF cat_count = 0 THEN                                       --When there's no pipes, value is atomic, no need to split
           EXECUTE IMMEDIATE inssql
           USING rw.column1, rw.column3;
        ELSE
            cat_val := REGEXP_SUBSTR(rw.column3, '^[^\|]+', 1);     --Find first cat value from start of the col till first '|'
            EXECUTE IMMEDIATE inssql
            USING rw.column1, cat_val;
            FOR i IN 1..cat_count LOOP                              --To capture remaining cat vals with given pattern
                EXIT WHEN i = cat_count;                            --Last val needs special pattern as shown below
                cat_val := REGEXP_SUBSTR(rw.column3, '[^\|]+', INSTR(rw.COLUMN3,'|',1,i),1);
                EXECUTE IMMEDIATE inssql
                USING rw.column1, cat_val;
            END LOOP;
            cat_val := REGEXP_SUBSTR(rw.column3, '[^\|]+$', INSTR(rw.COLUMN3,'|',1,cat_count),1);  --Captures last cat in a col
            EXECUTE IMMEDIATE inssql
            USING rw.column1, cat_val;
        END IF;
        row_count := split_genre%ROWCOUNT;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(row_count || ' records processed');
END insert_cat_tbl;
/

--Procedure for creating age decode table
CREATE OR REPLACE PROCEDURE crt_age_tbl IS
    drpsql      VARCHAR2(1000):='DROP TABLE age_decode CASCADE CONSTRAINTS PURGE';
    crtsql      VARCHAR2(1000):='CREATE TABLE age_decode (age_code    VARCHAR2(2) CONSTRAINT age_pk PRIMARY KEY NOT NULL,
                                                          age_group   VARCHAR2(10))';
BEGIN
    DBMS_OUTPUT.PUT_LINE('Dropping age_decode table');
    EXECUTE IMMEDIATE drpsql;
    DBMS_OUTPUT.PUT_LINE('Creating age_decode table');
    EXECUTE IMMEDIATE crtsql;
    DBMS_OUTPUT.PUT_LINE('Created age_decode table');
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Nothing to drop');
        DBMS_OUTPUT.PUT_LINE('Creating age_decode table');
        EXECUTE IMMEDIATE crtsql;
        DBMS_OUTPUT.PUT_LINE('Created age_decode table');
END crt_age_tbl;
/

--Procedure for loading age decode table
CREATE OR REPLACE PROCEDURE insert_age_tbl IS
    inssql  VARCHAR2(1000):='INSERT INTO age_decode VALUES(:a,:b)';
BEGIN
    DBMS_OUTPUT.PUT_LINE('Loading Age Table');
    EXECUTE IMMEDIATE inssql
    USING '1', 'Under 18';
    EXECUTE IMMEDIATE inssql
    USING '18', '18-24';
    EXECUTE IMMEDIATE inssql
    USING '25', '25-34';
    EXECUTE IMMEDIATE inssql
    USING '35', '35-44';
    EXECUTE IMMEDIATE inssql
    USING '45', '45-49';
    EXECUTE IMMEDIATE inssql
    USING '50', '50-55';
    EXECUTE IMMEDIATE inssql
    USING '56', '56+';
    DBMS_OUTPUT.PUT_LINE('Age Table Loading Complete');
END insert_age_tbl;
/

--Procedure for creating occupation decode table
CREATE OR REPLACE PROCEDURE crt_occu_tbl IS
    drpsql      VARCHAR2(1000):='DROP TABLE occupation_decode CASCADE CONSTRAINTS PURGE';
    crtsql      VARCHAR2(1000):='CREATE TABLE occupation_decode (occupation_code VARCHAR2(2) CONSTRAINT occu_pk PRIMARY KEY NOT NULL,
    occupation_group  VARCHAR2(35)
    )';
BEGIN
    DBMS_OUTPUT.PUT_LINE('Dropping occupation_decode table');
    EXECUTE IMMEDIATE drpsql;
    DBMS_OUTPUT.PUT_LINE('Creating occupation_decode table');
    EXECUTE IMMEDIATE crtsql;
    DBMS_OUTPUT.PUT_LINE('Created occupation_decode table');
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Nothing to drop');
        DBMS_OUTPUT.PUT_LINE('Creating occupation_decode table');
        EXECUTE IMMEDIATE crtsql;
        DBMS_OUTPUT.PUT_LINE('Created occupation_decode table');
END crt_occu_tbl;
/

--Procedure for loading occupation decode table
CREATE OR REPLACE PROCEDURE insert_occu_tbl IS
    inssql  VARCHAR2(1000):='INSERT INTO occupation_decode VALUES(:a,:b)';
BEGIN
    DBMS_OUTPUT.PUT_LINE('Loading Occupation Table');
    EXECUTE IMMEDIATE inssql
    USING '0','other';
    EXECUTE IMMEDIATE inssql
    USING '1','academic/educator';
    EXECUTE IMMEDIATE inssql
    USING '2','artist';
    EXECUTE IMMEDIATE inssql
    USING '3','clerical/admin';
    EXECUTE IMMEDIATE inssql
    USING '4','college/grad student';
    EXECUTE IMMEDIATE inssql 
    USING '5','customer service';
    EXECUTE IMMEDIATE inssql 
    USING '6','doctor/health care';
    EXECUTE IMMEDIATE inssql 
    USING '7','executive/managerial';
    EXECUTE IMMEDIATE inssql 
    USING '8','farmer';
    EXECUTE IMMEDIATE inssql 
    USING '9','homemaker';
    EXECUTE IMMEDIATE inssql 
    USING '10','K-12 student';
    EXECUTE IMMEDIATE inssql 
    USING '11','lawyer';
    EXECUTE IMMEDIATE inssql 
    USING '12','programmer';
    EXECUTE IMMEDIATE inssql 
    USING '13','retired';
    EXECUTE IMMEDIATE inssql 
    USING '14','sales/marketing';
    EXECUTE IMMEDIATE inssql 
    USING '15','scientist';
    EXECUTE IMMEDIATE inssql 
    USING '16','self-employed';
    EXECUTE IMMEDIATE inssql 
    USING '17','technician/engineer';
    EXECUTE IMMEDIATE inssql 
    USING '18','tradesman/craftsman';
    EXECUTE IMMEDIATE inssql 
    USING '19','unemployed';
    EXECUTE IMMEDIATE inssql 
    USING '20','writer';
    DBMS_OUTPUT.PUT_LINE('Occupation Table Loading Complete');
END insert_occu_tbl;
/

--Calling procedures to create the DB and load data
call crt_occu_tbl();
call insert_occu_tbl();
call crt_age_tbl();
call insert_age_tbl();
call crt_movie_tbl();
call insert_movie_tbl();
call crt_cat_tbl();
call insert_cat_tbl();
call crt_usr_tbl();
call insert_usr_tbl();
call crt_rat_tbl();
call insert_rat_tbl();



--Analysis queries
--Below query identifies gender and age group of users with ratings data
--Based on the output of the below query, only 25% of the ratings were by Females
SELECT a.gender, b.age_group, count(c.rating) rating_cnts
FROM users a
INNER JOIN age_decode b
on (a.age = b.age_code)
LEFT JOIN ratings c
on (a.user_id = c.user_id)
group by a.gender, b.age_group
;

--Below query identifies movie categories highly rated by male respondants 
--Drama, Comedy, and Action ranked highly
SELECT b.movie_cat, count(c.user_id) user_cnt
FROM ratings a
INNER JOIN movie_cat b
ON (a.movie_id = b.movie_id)
INNER JOIN users c
ON (a.user_id = c.user_id)
WHERE c.gender = 'M'
  AND a.rating > 3
GROUP BY b.movie_cat
ORDER BY 1;

--Below query identifies movie categories highly rated by male respondants 
--Drama, Comedy, and Romance ranked highly
SELECT b.movie_cat, count(c.user_id) user_cnt
FROM ratings a
INNER JOIN movie_cat b
ON (a.movie_id = b.movie_id)
INNER JOIN users c
ON (a.user_id = c.user_id)
WHERE c.gender = 'F'
  AND a.rating > 3
GROUP BY b.movie_cat
ORDER BY 1;
