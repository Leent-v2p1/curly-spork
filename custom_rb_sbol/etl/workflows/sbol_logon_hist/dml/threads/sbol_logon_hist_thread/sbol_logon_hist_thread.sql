let typeWf = '${$type}';
let parallelDegree = '${$parallel_degree}';
let ctlLoading = '${$loading_id}';
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("type - $typeWf");
print("parallelDegree - $parallelDegree");
print("ctlLoading - $ctlLoading");
print("businessDate - $businessDate");

TRY
delete_hdfs_dir('/data/custom/rb/sbol/stg/sbol_logon_hist_stg');
CATCH ex THEN
    print($ex['message']);
END
DROP TABLE IF EXISTS custom_rb_sbol_stg.sbol_logon_hist_stg;
CREATE EXTERNAL TABLE custom_rb_sbol_stg.sbol_logon_hist_stg(
  epk_id bigint,
  fst_logon_ever_dt timestamp,
  lst_logon_ever_dt timestamp,
  fst_mobil_ever_dt timestamp,
  lst_mobil_ever_dt timestamp,
  fst_web_ever_dt timestamp,
  lst_web_ever_dt timestamp,
  fst_atm_ever_dt timestamp,
  lst_atm_ever_dt timestamp,
  fst_arm_ever_dt timestamp,
  lst_arm_ever_dt timestamp,
  fst_sberid_ever_dt timestamp,
  lst_sberid_ever_dt timestamp,
  login_id decimal(38,0),
  device_info string,
  application string,
  channel_type string,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/stg/sbol_logon_hist_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_sbol_stg.sbol_logon_hist_stg
WITH base AS (
 SELECT *,
   CASE WHEN upper(application) = 'PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END as channel_type_norm,
   CASE
     WHEN channel_type IS NOT NULL THEN channel_type
     WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI'
     WHEN upper(application) = 'PHIZ_IC' THEN 'WEB'
     WHEN upper(application) LIKE '%ATM%' THEN 'ATM'
     WHEN upper(application) = 'PHIZ_IA' THEN 'SERVICE'
     ELSE NULL
   END as channel_type_all
 FROM (
   SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl
   UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl2
   UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl3
   UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl4
   UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl5
   UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl6
   UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl7
   UNION ALL SELECT * FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl_gf
 ) x
 WHERE epk_id IS NOT NULL AND cast(epk_id as bigint) <> -1 AND day_part < date_format(current_date(),'yyyy-MM-dd')
), aggr AS (
 SELECT
   cast(epk_id as bigint) as epk_id,
   min(logon_dt) as fst_logon_ever_dt,
   max(logon_dt) as lst_logon_ever_dt,
   min(case when upper(channel_type_all) in ('MAPI','MB') then logon_dt end) as fst_mobil_ever_dt,
   max(case when upper(channel_type_all) in ('MAPI','MB') then logon_dt end) as lst_mobil_ever_dt,
   min(case when upper(channel_type_all) in ('WEB','CSA') then logon_dt end) as fst_web_ever_dt,
   max(case when upper(channel_type_all) in ('WEB','CSA') then logon_dt end) as lst_web_ever_dt,
   min(case when upper(channel_type_all) in ('ATM','TERMINAL') then logon_dt end) as fst_atm_ever_dt,
   max(case when upper(channel_type_all) in ('ATM','TERMINAL') then logon_dt end) as lst_atm_ever_dt,
   min(case when upper(channel_type_all) = 'SERVICE' then logon_dt end) as fst_arm_ever_dt,
   max(case when upper(channel_type_all) = 'SERVICE' then logon_dt end) as lst_arm_ever_dt,
   min(case when upper(channel_type_all) = 'INTERNET_BANK_SBERID' then logon_dt end) as fst_sberid_ever_dt,
   max(case when upper(channel_type_all) = 'INTERNET_BANK_SBERID' then logon_dt end) as lst_sberid_ever_dt
 FROM base GROUP BY cast(epk_id as bigint)
), last_log AS (
 SELECT * FROM (
   SELECT cast(epk_id as bigint) as epk_id, login_id, device_info, application, channel_type_norm as channel_type, tb_nmb, osb_nmb, vsp_nmb, schema,
          row_number() over(partition by cast(epk_id as bigint) order by logon_dt desc) as rn
   FROM base
 ) q WHERE rn = 1
)
SELECT
  aggr.epk_id, aggr.fst_logon_ever_dt, aggr.lst_logon_ever_dt, aggr.fst_mobil_ever_dt, aggr.lst_mobil_ever_dt,
  aggr.fst_web_ever_dt, aggr.lst_web_ever_dt, aggr.fst_atm_ever_dt, aggr.lst_atm_ever_dt, aggr.fst_arm_ever_dt,
  aggr.lst_arm_ever_dt, aggr.fst_sberid_ever_dt, aggr.lst_sberid_ever_dt, last_log.login_id, last_log.device_info,
  last_log.application, last_log.channel_type, last_log.tb_nmb, last_log.osb_nmb, last_log.vsp_nmb, last_log.schema,
  $ctlLoading as ctl_loading, current_timestamp() as ctl_validfrom, 'I' as ctl_action
FROM aggr LEFT JOIN last_log ON aggr.epk_id = last_log.epk_id;

coalesce_files('/data/custom/rb/sbol/stg/sbol_logon_hist_stg','/data/custom/rb/sbol/stg/sbol_logon_hist_temp',128,'snappy',CAST($parallelDegree as INT));
move_table_to_schema(
  s2tTableList='sbol_logon_hist_stg->sbol_logon_hist', srcSchema='custom_rb_sbol_stg', tgtSchema='custom_rb_sbol', compressionType='snappy', workMode='arc', truncateIncFilterList='', truncateArcFilterList='sbol_logon_hist->all', instanceFilter='', truncateStgFromPa=true
);
let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "arc"}';
publish($meta);
