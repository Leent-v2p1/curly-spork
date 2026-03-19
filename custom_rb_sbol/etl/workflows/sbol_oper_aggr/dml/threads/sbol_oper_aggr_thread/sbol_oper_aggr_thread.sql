let typeWf = '${$type}';
let parallelDegree = '${$parallel_degree}';
let ctlLoading = '${$loading_id}';
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("type - $typeWf");
print("parallelDegree - $parallelDegree");
print("ctlLoading - $ctlLoading");
print("businessDate - $businessDate");

let startDt = '${$start_dt}';
if $startDt == '' then
    let startDt = '2015-01-01';
end if;
let firstDayCurrentMonth = (SELECT date_format(trunc(current_date(), 'MM'), 'yyyy-MM-dd'))[0][0];
TRY
delete_hdfs_dir('/data/custom/rb/sbol/stg/sbol_oper_aggr_stg');
CATCH ex THEN
    print($ex['message']);
END
DROP TABLE IF EXISTS custom_rb_sbol_stg.sbol_oper_aggr_stg;
CREATE EXTERNAL TABLE custom_rb_sbol_stg.sbol_oper_aggr_stg(
  epk_id bigint, login_type string, exec_rub_total decimal(38,15), exec_rub_web decimal(38,15), exec_rub_mobil decimal(38,15), exec_rub_atm decimal(38,15), exec_rub_mb decimal(38,15), exec_rub_other decimal(38,15), exec_qty bigint, exec_web_qty bigint, exec_mobil_qty bigint, exec_atm_qty bigint, exec_mb_qty bigint, exec_other_qty bigint, fst_exec_dt string, lst_exec_dt string, ctl_loading int, ctl_validfrom timestamp, ctl_action string
) PARTITIONED BY (month_part string) STORED AS PARQUET LOCATION '/data/custom/rb/sbol/stg/sbol_oper_aggr_stg' TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_sbol_stg.sbol_oper_aggr_stg PARTITION(month_part)
WITH base AS (
  SELECT * FROM (
    SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl
    UNION ALL SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl2
    UNION ALL SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl3
    UNION ALL SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl4
    UNION ALL SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl5
    UNION ALL SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl6
    UNION ALL SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl7
    UNION ALL SELECT * FROM ${$src_schema_name}.sbol_oper_ikfl_gf
  ) x
  WHERE epk_id IS NOT NULL AND oper_state_code = 'EXECUTED' AND oper_amt > 0 AND day_part >= '$startDt' AND day_part < '$firstDayCurrentMonth'
), aggr AS (
  SELECT coalesce(cast(epk_id as bigint), -1) as epk_id, coalesce(login_type,'') as login_type, substr(date_create,1,7) as month_part,
         min(date_create) as fst_exec_dt, max(date_create) as lst_exec_dt,
         cast(sum(oper_rur_amt) as decimal(38,15)) as exec_rub_total, count(*) as exec_qty,
         cast(sum(case when login_type='CSA' then oper_rur_amt else 0 end) as decimal(38,15)) as exec_rub_web,
         cast(sum(case when login_type='MAPI' then oper_rur_amt else 0 end) as decimal(38,15)) as exec_rub_mobil,
         cast(sum(case when login_type='TERMINAL' then oper_rur_amt else 0 end) as decimal(38,15)) as exec_rub_atm,
         cast(sum(case when login_type='MB' then oper_rur_amt else 0 end) as decimal(38,15)) as exec_rub_mb,
         cast(sum(case when coalesce(login_type,'') not in ('CSA','MAPI','TERMINAL','MB') then oper_rur_amt else 0 end) as decimal(38,15)) as exec_rub_other,
         sum(case when login_type='CSA' then 1 else 0 end) as exec_web_qty,
         sum(case when login_type='MAPI' then 1 else 0 end) as exec_mobil_qty,
         sum(case when login_type='TERMINAL' then 1 else 0 end) as exec_atm_qty,
         sum(case when login_type='MB' then 1 else 0 end) as exec_mb_qty,
         sum(case when coalesce(login_type,'') not in ('CSA','MAPI','TERMINAL','MB') then 1 else 0 end) as exec_other_qty
  FROM base GROUP BY coalesce(cast(epk_id as bigint), -1), coalesce(login_type,''), substr(date_create,1,7)
)
SELECT epk_id,
       case when login_type='' then null else login_type end as login_type,
       exec_rub_total, exec_rub_web, exec_rub_mobil, exec_rub_atm, exec_rub_mb, exec_rub_other, exec_qty, exec_web_qty, exec_mobil_qty, exec_atm_qty, exec_mb_qty, exec_other_qty, fst_exec_dt, lst_exec_dt,
       $ctlLoading as ctl_loading, current_timestamp() as ctl_validfrom, 'I' as ctl_action, month_part
FROM aggr;

coalesce_files('/data/custom/rb/sbol/stg/sbol_oper_aggr_stg','/data/custom/rb/sbol/stg/sbol_oper_aggr_temp',128,'snappy',CAST($parallelDegree as INT));
move_table_to_schema(
  s2tTableList='sbol_oper_aggr_stg->sbol_oper_aggr', srcSchema='custom_rb_sbol_stg', tgtSchema='custom_rb_sbol', compressionType='snappy', workMode='arc', truncateIncFilterList='', truncateArcFilterList='sbol_oper_aggr->all', instanceFilter='', truncateStgFromPa=true
);
let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "arc"}';
publish($meta);
