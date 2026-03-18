select count(distinct(epk_id)) > 110000000
from custom_rb_sbol_stg.sbol_logon_hist_reserve

--//--

select check_unq_pk >= 97
from(
select count(distinct(epk_id))/count(*)*100 as check_unq_pk
from custom_rb_sbol_stg.sbol_logon_hist_reserve
)t

--//--

select min(fst_logon_ever_dt) < '2018-01-01'
FROM custom_rb_sbol_stg.sbol_logon_hist_reserve