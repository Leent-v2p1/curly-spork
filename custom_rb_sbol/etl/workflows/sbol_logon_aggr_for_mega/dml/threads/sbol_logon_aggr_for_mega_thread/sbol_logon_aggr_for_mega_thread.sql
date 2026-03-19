let typeWf = '${$type}';
let parallelDegree = '${$parallel_degree}';
let ctlLoading = '${$loading_id}';
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("type - $typeWf");
print("parallelDegree - $parallelDegree");
print("ctlLoading - $ctlLoading");
print("businessDate - $businessDate");

TRY
delete_hdfs_dir('/data/custom/rb/sbol/stg/sbol_logon_aggr_for_mega_stg');
CATCH ex THEN
    print($ex['message']);
END
DROP TABLE IF EXISTS custom_rb_sbol_stg.sbol_logon_aggr_for_mega_stg;
CREATE EXTERNAL TABLE custom_rb_sbol_stg.sbol_logon_aggr_for_mega_stg(
  epk_id bigint,
  sbol_fst_login_ever_dt string,
  sbol_atm_fst_login_ever_dt string,
  sbol_mob_fst_login_ever_dt string,
  sbol_web_fst_login_ever_dt string,
  sbol_lst_login_ever_dt string,
  sbol_atm_lst_login_ever_dt string,
  sbol_mob_lst_login_ever_dt string,
  sbol_web_lst_login_ever_dt string,
  sbol_fst_login_dt timestamp,
  sbol_atm_fst_login_dt timestamp,
  sbol_mob_fst_login_dt timestamp,
  sbol_web_fst_login_dt timestamp,
  sbol_lst_login_dt timestamp,
  sbol_atm_lst_login_dt timestamp,
  sbol_mob_lst_login_dt timestamp,
  sbol_web_lst_login_dt timestamp,
  sbol_1m_login_flag int,
  sbol_atm_1m_login_flag int,
  sbol_mob_1m_login_flag int,
  sbol_web_1m_login_flag int,
  sbol_3m_login_flag int,
  sbol_atm_3m_login_flag int,
  sbol_mob_3m_login_flag int,
  sbol_web_3m_login_flag int,
  sbol_mnth_fst_dt_qty double,
  sbol_mnth_lst_dt_qty double,
  sbol_mnth_atm_lst_dt_qty double,
  sbol_mnth_mob_lst_dt_qty double,
  sbol_mnth_web_lst_dt_qty double,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (month_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/stg/sbol_logon_aggr_for_mega_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_sbol_stg.sbol_logon_aggr_for_mega_stg PARTITION(month_part)
WITH src AS (
  SELECT *,
    CASE WHEN tot_qty > 0 THEN 1 ELSE 0 END AS sbol_1m_login_flag,
    CASE WHEN atm_qty > 0 THEN 1 ELSE 0 END AS sbol_atm_1m_login_flag,
    CASE WHEN mobil_qty > 0 THEN 1 ELSE 0 END AS sbol_mob_1m_login_flag,
    CASE WHEN web_qty > 0 THEN 1 ELSE 0 END AS sbol_web_1m_login_flag,
    CASE WHEN sum(tot_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_3m_login_flag,
    CASE WHEN sum(atm_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_atm_3m_login_flag,
    CASE WHEN sum(mobil_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_mob_3m_login_flag,
    CASE WHEN sum(web_qty) OVER (PARTITION BY epk_id ORDER BY month_part ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) > 0 THEN 1 ELSE 0 END AS sbol_web_3m_login_flag
  FROM custom_rb_sbol.sbol_logon_aggr
  WHERE month_part < date_format(current_date(),'yyyy-MM')
), ext AS (
  SELECT
    epk_id,
    cast(min(fst_logon_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_fst_login_ever_dt,
    cast(min(fst_atm_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_atm_fst_login_ever_dt,
    cast(min(fst_mobil_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_mob_fst_login_ever_dt,
    cast(min(fst_web_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_web_fst_login_ever_dt,
    cast(max(lst_logon_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_lst_login_ever_dt,
    cast(max(lst_atm_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_atm_lst_login_ever_dt,
    cast(max(lst_mobil_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_mob_lst_login_ever_dt,
    cast(max(lst_web_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_web_lst_login_ever_dt,
    fst_logon_dt as sbol_fst_login_dt,
    fst_atm_dt as sbol_atm_fst_login_dt,
    fst_mobil_dt as sbol_mob_fst_login_dt,
    fst_web_dt as sbol_web_fst_login_dt,
    lst_logon_dt as sbol_lst_login_dt,
    lst_atm_dt as sbol_atm_lst_login_dt,
    lst_mobil_dt as sbol_mob_lst_login_dt,
    lst_web_dt as sbol_web_lst_login_dt,
    sbol_1m_login_flag,
    sbol_atm_1m_login_flag,
    sbol_mob_1m_login_flag,
    sbol_web_1m_login_flag,
    sbol_3m_login_flag,
    sbol_atm_3m_login_flag,
    sbol_mob_3m_login_flag,
    sbol_web_3m_login_flag,
    months_between(date_add(add_months(concat(month_part,'-01'),1),-1), cast(min(fst_logon_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as timestamp)) as sbol_mnth_fst_dt_qty,
    months_between(date_add(add_months(concat(month_part,'-01'),1),-1), cast(max(lst_logon_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as timestamp)) as sbol_mnth_lst_dt_qty,
    months_between(date_add(add_months(concat(month_part,'-01'),1),-1), cast(max(lst_atm_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as timestamp)) as sbol_mnth_atm_lst_dt_qty,
    months_between(date_add(add_months(concat(month_part,'-01'),1),-1), cast(max(lst_mobil_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as timestamp)) as sbol_mnth_mob_lst_dt_qty,
    months_between(date_add(add_months(concat(month_part,'-01'),1),-1), cast(max(lst_web_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as timestamp)) as sbol_mnth_web_lst_dt_qty,
    month_part
  FROM src
)
SELECT ext.*, $ctlLoading as ctl_loading, current_timestamp() as ctl_validfrom, 'I' as ctl_action, month_part FROM ext;

coalesce_files('/data/custom/rb/sbol/stg/sbol_logon_aggr_for_mega_stg','/data/custom/rb/sbol/stg/sbol_logon_aggr_for_mega_temp',128,'snappy',CAST($parallelDegree as INT));
move_table_to_schema(
  s2tTableList='sbol_logon_aggr_for_mega_stg->sbol_logon_aggr_for_mega', srcSchema='custom_rb_sbol_stg', tgtSchema='custom_rb_sbol', compressionType='snappy', workMode='arc', truncateIncFilterList='', truncateArcFilterList='sbol_logon_aggr_for_mega->all', instanceFilter='', truncateStgFromPa=true
);
let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "arc"}';
publish($meta);
