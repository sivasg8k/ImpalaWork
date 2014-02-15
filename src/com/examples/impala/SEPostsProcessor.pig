post_data = LOAD '/input/posts' USING PigStorage(',') AS (id,post_type_id,acc_ans_id,creation_date,score,view_count,owner_user_id,ans_count,com_count,fav_count,closed_date);
post_data_mod = FOREACH post_data {
	closed_date_mod=(closed_date == 'NULL'?'':closed_date);
	GENERATE id,post_type_id,acc_ans_id,creation_date,score,view_count,owner_user_id,ans_count,com_count,fav_count,closed_date_mod;
}
dump post_data_mod;

