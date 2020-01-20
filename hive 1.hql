create external table if not exists genome_scores3(movieID int, tagID int, relevance double) row format delimited fields terminated by ',' location 'hdfs://localhost:54310/PFDA/hive_genome_scores3/';

LOAD DATA LOCAL INPATH '/media/hduser/I don\'t spin/PFDA Repeat Project/Map Reduce Files/new_genome_scores1' INTO TABLE genome_scores3;


create external table if not exists genome_tags1(tagID int, tag String) row format delimited fields terminated by ',' location 'hdfs://localhost:54310/PFDA/hive_genome_tags1/';

LOAD DATA LOCAL INPATH '/media/hduser/I don't spin/PFDA Repeat Project/Map Reduce Files/genome_tags' INTO TABLE genome_tags1;

insert overwrite directory 'hdfs://localhost:54310/PFDA/query4' row format delimited fields terminated by '\054' SELECT DISTINCT genome_scores2.movieID, movies.title, genome_scores2.tagID, genome_scores2.relevance, genome_tags.tag FROM genome_scores2 LEFT JOIN genome_tags ON genome_scores2.tagID=genome_tags.tagID LEFT JOIN movies  ON genome_scores2.movieID = movies.movieID WHERE genome_scores2.tagID IS NOT NULL ORDER BY genome_scores2.relevance desc LIMIT 1000;

sqoop import --connect jdbc:mysql://127.0.0.1/PFDA --username root --password microsoft --table genome_scores1 --target-dir /sqoop/genome_scores1 -m 1
