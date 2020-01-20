movies = LOAD '/media/hduser/I don\'t spin/PFDA Repeat Project/Map Reduce Files/movies' USING PigStorage(',') AS(movieID:int,title:chararray,genre:chararray);
ratings = LOAD '/media/hduser/I don\'t spin/PFDA Repeat Project/Map Reduce Files/ratings' USING PigStorage(',') AS(userID:int,movieID:int,rating:int);
j1 = JOIN movies BY movieID, ratings BY movieID;
j2 = FOREACH j1 GENERATE movies::movieID, title, genre, rating;
filt = filter j2 by ( genre matches '.*Comedy.*' ) ;
j2_avg = GROUP filt BY movies::movieID;
g2 = FOREACH j2_avg GENERATE group, AVG(filt.rating);
limit_data = limit g2 20;

store limit_data 'hdfs://localhost:54310/PFDA/query1
--ordered_movies = order g2 by rating desc;

