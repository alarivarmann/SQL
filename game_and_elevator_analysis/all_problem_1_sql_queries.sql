
-- CREATE TABLE public.all_comb as (
-- 	WITH myconstants (playeramount, ageamount) as (
--    values (10000, 178)
-- )
-- SELECT player_id,age FROM myconstants,
-- GENERATE_SERIES(1,ageamount ) as age,GENERATE_SERIES(1,playeramount ) as player_id
-- 	);




-- CREATE TABLE result1 as (
-- select DISTINCT m.player_id,level_completed_player_id,age,last_value(level_number) OVER (partition by  player_id,age) as last_level_per_age
-- from
-- (select i.player_id as player_id,l.player_id as level_completed_player_id,
-- DATE_PART('day', date_trunc('day',l.timestamp) - date_trunc('day',i.timestamp)) as age,
-- l.level_number

-- from installation_events as i
-- left join level_events as l
-- on i.player_id = l.player_id
--  )
-- as m
-- 	)
-- where level_completed_player_id IS NOT NULL

-- CREATE TABLE public.full as
-- (
-- SELECT a.player_id,
-- a.age,
-- r.last_level_per_age
-- FROM all_comb  as a
-- LEFT JOIN result1 as r
-- ON
-- a.player_id = r.player_id and
-- a.age = r.age
-- ORDER BY player_id,age ASC
-- ) ;



-- CREATE TABLE final_table as (
-- select player_id,age,first_value(new_level) over (partition by value_partition order by player_id,age) as player_true_level
-- from (
-- select 	player_id,age,new_level,
-- 	sum(case when new_level is null then 0 else 1 end) over (order by player_id,age) as value_partition

-- from (
-- 	select player_id,age,
-- 	CASE WHEN age = 1 and correct_level is null THEN 0 ELSE correct_level END as new_level
-- from (
-- SELECT player_id,
-- age,
--  CASE
--  WHEN player_id in
--  (select player_id from public.full group by 1 having bool_and(public.full.last_level_per_age is null))
--  THEN 0
--  ELSE  public.full.last_level_per_age  END
--  as correct_level,last_level_per_age

-- FROM public.full
-- ORDER BY player_id,age
-- --WINDOW w AS (ORDER BY player_id,age) ) as s
-- 	) as m
-- 	) as t
-- ) as k
-- )


--OR
-- CREATE TABLE final_table as (
-- WITH m as (
-- SELECT player_id,
-- age,
--  CASE
--  WHEN player_id in
--  (select player_id from public.full group by 1 having bool_and(public.full.last_level_per_age is null))
--  THEN 0
--  ELSE  public.full.last_level_per_age  END
--  as correct_level,last_level_per_age

-- FROM public.full
-- ORDER BY player_id,age
-- --WINDOW w AS (ORDER BY player_id,age) ) as s
-- 	),
-- t as (
-- 	select player_id,age,
-- 	CASE WHEN age = 1 and correct_level is null THEN 0 ELSE correct_level END as new_level
-- from m
-- 	),
-- k as (
-- select 	player_id,age,new_level,
-- 	sum(case when new_level is null then 0 else 1 end) over (order by player_id,age) as value_partition
-- from t
-- )
-- select player_id,age,first_value(new_level) over (partition by value_partition order by player_id,age) as player_true_level
-- from k
-- )
-- have to do the left join not to throw away any data

CREATE TABLE final_aggregate as (
select ft.age,
AVG(CAST (ft.player_true_level as Float)) as avg_level_all,
AVG(CAST (r.last_level_per_age as Float)) as avg_level_active
from final_table as ft
LEFT join
result1 as r on
ft.age = r.age
group by ft.age);


