DROP TABLE IF EXISTS comments;
CREATE EXTERNAL TABLE comments
(
   id BIGINT,
   post_id BIGINT,
   score INT,
   creation_date TIMESTAMP,
   user_id BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/input/comments';

DROP TABLE IF EXISTS posts;
CREATE EXTERNAL TABLE posts
(
   id BIGINT,
   post_type_id INT,
   acc_ans_id BIGINT,
   creation_date TIMESTAMP,
   score INT,
   view_count INT,
   owner_user_id BIGINT,
   ans_count INT,
   com_count INT,
   fav_count INT,
   closed_date TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/input/posts';

DROP TABLE IF EXISTS users;
CREATE EXTERNAL TABLE users
(
   id BIGINT,
   reputation INT,
   creation_date TIMESTAMP,
   display_name STRING,
   last_access_date TIMESTAMP,
   region_1 STRING,
   region_2 STRING,
   views INT,
   upvotes INT,
   downvotes INT,
   age INT
   
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/input/users';

DROP TABLE IF EXISTS votes;
CREATE EXTERNAL TABLE votes
(
   id INT,
   post_id BIGINT,
   vote_type_id INT,
   creation_date TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/input/votes';