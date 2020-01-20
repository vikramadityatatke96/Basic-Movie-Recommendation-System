movies = LOAD '/media/hduser/I don\'t spin/PFDA Repeat Project/Map Reduce Files/movies' USING PigStorage(',') AS(movieID:int,title:chararray,genre:chararray);
ratings = LOAD '/media/hduser/I don\'t spin/PFDA Repeat Project/Map Reduce Files/ratings' USING PigStorage(',') AS(userID:int,movieID:int,rating:int);
j1 = JOIN movies BY movieID, ratings BY movieID;
j2 = FOREACH j1 GENERATE movies::movieID, title, genre, rating;
filt1 = filter j2 by ( genre matches '.*Action.*' ) ;
j2_avg = GROUP filt1 BY movies::movieID;
g3 = FOREACH j2_avg GENERATE group, AVG(filt1.rating);
limit_data = limit g3 20;

store limit_data 'hdfs://localhost:54310/PFDA/query2
