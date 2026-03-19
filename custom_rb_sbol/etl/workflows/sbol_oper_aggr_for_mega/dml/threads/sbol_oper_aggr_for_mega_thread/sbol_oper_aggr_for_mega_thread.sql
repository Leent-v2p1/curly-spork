let typeWf = '${$type}';
let parallelDegree = '${$parallel_degree}';
let ctlLoading = '${$loading_id}';
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("type - $typeWf");
print("parallelDegree - $parallelDegree");
print("ctlLoading - $ctlLoading");
print("businessDate - $businessDate");

TRY
delete_hdfs_dir('/data/custom/rb/sbol/stg/sbol_oper_aggr_for_mega_stg');
CATCH ex THEN
    print($ex['message']);
END
DROP TABLE IF EXISTS custom_rb_sbol_stg.sbol_oper_aggr_for_mega_stg;
CREATE EXTERNAL TABLE custom_rb_sbol_stg.sbol_oper_aggr_for_mega_stg(
  epk_id bigint, sbol_fst_txn_dt string, sbol_lst_txn_dt string, sbol_fst_txn_ever_dt string, sbol_lst_txn_ever_dt string,
  sbol_txn_1m_amt decimal(38,15), sbol_txn_1m_qty bigint, sbol_txn_3m_amt decimal(38,15), sbol_txn_3m_qty bigint,
  sbol_txn_atm_1m_amt decimal(38,15), sbol_txn_atm_1m_qty bigint, sbol_txn_atm_3m_amt decimal(38,15), sbol_txn_atm_3m_qty bigint,
  sbol_txn_mob_1m_amt decimal(38,15), sbol_txn_mob_1m_qty bigint, sbol_txn_mob_3m_amt decimal(38,15), sbol_txn_mob_3m_qty bigint,
  sbol_txn_web_1m_amt decimal(38,15), sbol_txn_web_1m_qty bigint, sbol_txn_web_3m_amt decimal(38,15), sbol_txn_web_3m_qty bigint,
  sbol_txn_main_1m_amt decimal(38,15), sbol_txn_main_1m_qty bigint, sbol_txn_main_3m_amt decimal(38,15), sbol_txn_main_3m_qty bigint,
  sbol_txn_mb_1m_amt decimal(38,15), sbol_txn_mb_1m_qty bigint, sbol_txn_mb_3m_amt decimal(38,15), sbol_txn_mb_3m_qty bigint,
  sbol_txn_other_1m_amt decimal(38,15), sbol_txn_other_1m_qty bigint, sbol_txn_other_3m_amt decimal(38,15), sbol_txn_other_3m_qty bigint,
  sbol_mnth_fst_dt_qty double, sbol_mnth_lst_dt_qty double, ctl_loading int, ctl_validfrom timestamp, ctl_action string
) PARTITIONED BY (month_part string) STORED AS PARQUET LOCATION '/data/custom/rb/sbol/stg/sbol_oper_aggr_for_mega_stg' TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_sbol_stg.sbol_oper_aggr_for_mega_stg PARTITION(month_part)
WITH src AS (
  SELECT
    coalesce(cast(epk_id as bigint),-1) as epk_id,
    month_part,
    min(fst_exec_dt) as fst_exec_dt,
    max(lst_exec_dt) as lst_exec_dt,
    cast(sum(exec_rub_total) as decimal(38,15)) as exec_rub_total,
    sum(exec_qty) as exec_qty,
    cast(sum(exec_rub_atm) as decimal(38,15)) as exec_rub_atm,
    sum(exec_atm_qty) as exec_atm_qty,
    cast(sum(exec_rub_mobil) as decimal(38,15)) as exec_rub_mobil,
    sum(exec_mobil_qty) as exec_mobil_qty,
    cast(sum(exec_rub_web) as decimal(38,15)) as exec_rub_web,
    sum(exec_web_qty) as exec_web_qty,
    cast(sum(exec_rub_atm + exec_rub_mobil + exec_rub_web) as decimal(38,15)) as exec_rub_main,
    sum(exec_atm_qty + exec_mobil_qty + exec_web_qty) as exec_main_qty,
    cast(sum(exec_rub_mb) as decimal(38,15)) as exec_rub_mb,
    sum(exec_mb_qty) as exec_mb_qty,
    cast(sum(exec_rub_other) as decimal(38,15)) as exec_rub_other,
    sum(exec_other_qty) as exec_other_qty
  FROM custom_rb_sbol.sbol_oper_aggr
  WHERE month_part < date_format(current_date(),'yyyy-MM')
  GROUP BY coalesce(cast(epk_id as bigint),-1), month_part
), ext AS (
  SELECT
    epk_id, month_part, fst_exec_dt as sbol_fst_txn_dt, lst_exec_dt as sbol_lst_txn_dt,
    cast(min(fst_exec_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_fst_txn_ever_dt,
    cast(max(lst_exec_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as string) as sbol_lst_txn_ever_dt,
    exec_rub_total as sbol_txn_1m_amt, exec_qty as sbol_txn_1m_qty,
    cast(sum(exec_rub_total) over(partition by epk_id order by month_part rows between 2 preceding and current row) as decimal(38,15)) as sbol_txn_3m_amt,
    sum(exec_qty) over(partition by epk_id order by month_part rows between 2 preceding and current row) as sbol_txn_3m_qty,
    exec_rub_atm as sbol_txn_atm_1m_amt, exec_atm_qty as sbol_txn_atm_1m_qty,
    cast(sum(exec_rub_atm) over(partition by epk_id order by month_part rows between 2 preceding and current row) as decimal(38,15)) as sbol_txn_atm_3m_amt,
    sum(exec_atm_qty) over(partition by epk_id order by month_part rows between 2 preceding and current row) as sbol_txn_atm_3m_qty,
    exec_rub_mobil as sbol_txn_mob_1m_amt, exec_mobil_qty as sbol_txn_mob_1m_qty,
    cast(sum(exec_rub_mobil) over(partition by epk_id order by month_part rows between 2 preceding and current row) as decimal(38,15)) as sbol_txn_mob_3m_amt,
    sum(exec_mobil_qty) over(partition by epk_id order by month_part rows between 2 preceding and current row) as sbol_txn_mob_3m_qty,
    exec_rub_web as sbol_txn_web_1m_amt, exec_web_qty as sbol_txn_web_1m_qty,
    cast(sum(exec_rub_web) over(partition by epk_id order by month_part rows between 2 preceding and current row) as decimal(38,15)) as sbol_txn_web_3m_amt,
    sum(exec_web_qty) over(partition by epk_id order by month_part rows between 2 preceding and current row) as sbol_txn_web_3m_qty,
    exec_rub_main as sbol_txn_main_1m_amt, exec_main_qty as sbol_txn_main_1m_qty,
    cast(sum(exec_rub_main) over(partition by epk_id order by month_part rows between 2 preceding and current row) as decimal(38,15)) as sbol_txn_main_3m_amt,
    sum(exec_main_qty) over(partition by epk_id order by month_part rows between 2 preceding and current row) as sbol_txn_main_3m_qty,
    exec_rub_mb as sbol_txn_mb_1m_amt, exec_mb_qty as sbol_txn_mb_1m_qty,
    cast(sum(exec_rub_mb) over(partition by epk_id order by month_part rows between 2 preceding and current row) as decimal(38,15)) as sbol_txn_mb_3m_amt,
    sum(exec_mb_qty) over(partition by epk_id order by month_part rows between 2 preceding and current row) as sbol_txn_mb_3m_qty,
    exec_rub_other as sbol_txn_other_1m_amt, exec_other_qty as sbol_txn_other_1m_qty,
    cast(sum(exec_rub_other) over(partition by epk_id order by month_part rows between 2 preceding and current row) as decimal(38,15)) as sbol_txn_other_3m_amt,
    sum(exec_other_qty) over(partition by epk_id order by month_part rows between 2 preceding and current row) as sbol_txn_other_3m_qty,
    months_between(date_add(add_months(concat(month_part,'-01'),1),-1), cast(min(fst_exec_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as timestamp)) as sbol_mnth_fst_dt_qty,
    months_between(date_add(add_months(concat(month_part,'-01'),1),-1), cast(max(lst_exec_dt) over(partition by epk_id order by month_part rows between unbounded preceding and current row) as timestamp)) as sbol_mnth_lst_dt_qty
  FROM src
)
SELECT ext.*, $ctlLoading as ctl_loading, current_timestamp() as ctl_validfrom, 'I' as ctl_action, month_part FROM ext;

coalesce_files('/data/custom/rb/sbol/stg/sbol_oper_aggr_for_mega_stg','/data/custom/rb/sbol/stg/sbol_oper_aggr_for_mega_temp',128,'snappy',CAST($parallelDegree as INT));
move_table_to_schema(
  s2tTableList='sbol_oper_aggr_for_mega_stg->sbol_oper_aggr_for_mega', srcSchema='custom_rb_sbol_stg', tgtSchema='custom_rb_sbol', compressionType='snappy', workMode='arc', truncateIncFilterList='', truncateArcFilterList='sbol_oper_aggr_for_mega->all', instanceFilter='', truncateStgFromPa=true
);
let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "arc"}';
publish($meta);
