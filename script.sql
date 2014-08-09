CREATE OR REPLACE FUNCTION script()
RETURNS INTEGER AS
$$
DECLARE _name VARCHAR;
DECLARE _round VARCHAR;
DECLARE _roundselector INTEGER;
DECLARE _nameselector INTEGER;
DECLARE _playercount INTEGER;
DECLARE _year INTEGER;
DECLARE _position INTEGER;
BEGIN
CREATE TABLE cup_matches (mid SERIAL PRIMARY KEY, round TEXT, year INT, num_ratings INT, rating FLOAT);
CREATE TABLE played_in(mid INT REFERENCES cup_matches(mid), name VARCHAR(20), Primary Key(mid,name), year INT, position INT);


FOR i IN 1..2680
LOOP
       SELECT INTO _roundselector trunc(random() * 5 + 1);
          SELECT INTO _round 
          CASE  _roundselector
             WHEN  1 THEN '16th'
             WHEN  2 THEN '32nd'
             WHEN  3 THEN 'QuarterFinal'
             WHEN  4 THEN 'SemiFinal'
             WHEN  5 THEN 'Final'
              END;
                
              _playercount:=22;
              _year:=trunc(random() * 49 + 1960);
          
          INSERT INTO cup_matches (rating,num_ratings,year,mid,round) (SELECT random() * 9 + 1, 
           	trunc(random() * 5 + 1), _year,i,_round);

           
           IF ((i%23)=1 OR i=2680)
            THEN  _playercount:=21;
                INSERT INTO played_in(mid,name,year,position) VALUES (i,'pele',_year,12);

        END IF;

           _nameselector:=trunc(random() * 8 + 1);
           FOR j IN 1.._playercount
           LOOP
                 
                    SELECT INTO _name
                      CASE _nameselector+j
                      WHEN 1 THEN 'Messi'
                      WHEN 2 THEN 'Ronaldo'
                      WHEN 3 THEN 'Gareth Bale'
                      WHEN 4 THEN 'Bi Maria'
                      WHEN 5 THEN 'Karim Benzema'
                      WHEN 6 THEN 'Ahmed Masry'
                      WHEN 7 THEN 'Tevez'
                      WHEN 8 THEN 'Casilas'
                      WHEN 9 THEN 'Puyol'
                      WHEN 10 THEN 'Pique'
                      WHEN 11 THEN 'Xabi Alonzo'
                      WHEN 12 THEN 'Iniesta'
                      WHEN 13 THEN 'Ibrahimovich'
                      WHEN 14 THEN 'Ronaldinho'
                      WHEN 15 THEN 'Zidane'
                      WHEN 16 THEN 'Maradona'
                      WHEN 17 THEN 'Beckham'
                      WHEN 18 THEN 'Kaka'
                      WHEN 19 THEN 'Figo'
                      WHEN 20 THEN 'Raul'
                      WHEN 21 THEN 'Rooney'
                      WHEN 22 THEN 'Torres'
                      WHEN 23 THEN 'Xavi'
                      WHEN 24 THEN 'Roberto Carlos'
                      WHEN 25 THEN 'Zico'
                      WHEN 26 THEN 'Platini'
                      WHEN 27 THEN 'Gerd Muller'
                      WHEN 28 THEN 'Franz Beckenbauer'
                      WHEN 29 THEN 'Alfredo di Stefano'
                      WHEN 30 THEN 'Alvaro Recoba'
                      END;
                
               
                _position:=((trunc(random() * 29 + 1)::INTEGER)%12)+1; 
                INSERT INTO played_in(mid,name,year,position) VALUES (i,_name,_year,_position);


           END LOOP;



    

 END LOOP;
 RETURN 1;
END;
$$
LANGUAGE plpgsql VOLATILE;
