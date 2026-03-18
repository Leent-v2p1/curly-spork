select check_unq_pk>=97
from(
select count(distinct(sav_goal_id))/count(*)*100 as check_unq_pk
from custom_rb_sbol_stg.sbol_sav_goal_ikfl7_reserve
)t


--//--

select cnt_epk>95
from(
SELECT sum(case when epk_id<>'-1' then 1 else 0 end)/ count(*)*100 as cnt_epk
FROM custom_rb_sbol_stg.sbol_sav_goal_ikfl7_reserve
)t