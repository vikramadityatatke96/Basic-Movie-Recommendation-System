create external table if not exists tags1(userID int, movieID int, tag String) row format delimited fields terminated by ',' location 'hdfs://localhost:54310/PFDA/hive_tags1/';

LOAD DATA LOCAL INPATH '/media/hduser/I don\'t spin/PFDA Repeat Project/Map Reduce Files/tags' INTO TABLE tags1;


create external table if not exists movies1(movieID int, title String, genre String, year int) row format delimited fields terminated by ',' location 'hdfs://localhost:54310/PFDA/hive_movies1/';

LOAD DATA LOCAL INPATH '/media/hduser/I don\'t spin/PFDA Repeat Project/Map Reduce Files/movies_year' INTO TABLE movies1;

insert overwrite directory 'hdfs://localhost:54310/PFDA/query3' row format delimited fields terminated by '\054' SELECT movies1.movieID, movies1.title, COUNT(*) as cnt FROM tags1 LEFT JOIN movies1 ON tags1.movieID=movies1.movieID WHERE movies1.movieID IS NOT NULL GROUP BY movies1.movieID, movies1.title ORDER BY cnt desc LIMIT 20; 
