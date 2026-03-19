let typeWf = '${$type}';
let parallelDegree = '${$parallel_degree}';
let ctlLoading = '${$loading_id}';
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("type - $typeWf");
print("parallelDegree - $parallelDegree");
print("ctlLoading - $ctlLoading");
print("businessDate - $businessDate");

let startDt = '${$start_dt}';
let endDt = '${$end_dt}';
if $startDt == '' then
    let startDt = '2015-01-01';
end if;
if $endDt == '' then
    let endDt = (SELECT date_format(current_timestamp(), 'yyyy-MM-dd'))[0][0];
end if;
let firstDayCurrentMonth = (SELECT date_format(trunc(current_date(), 'MM'), 'yyyy-MM-dd'))[0][0];

TRY
delete_hdfs_dir('/data/custom/rb/sbol/stg/sbol_logon_aggr_stg');
CATCH ex THEN
    print($ex['message']);
END
DROP TABLE IF EXISTS custom_rb_sbol_stg.sbol_logon_aggr_stg;
CREATE EXTERNAL TABLE custom_rb_sbol_stg.sbol_logon_aggr_stg(
  epk_id bigint,
  fst_logon_dt timestamp,
  lst_logon_dt timestamp,
  tot_qty bigint,
  fst_mobil_dt timestamp,
  lst_mobil_dt timestamp,
  mobil_qty bigint,
  fst_web_dt timestamp,
  lst_web_dt timestamp,
  web_qty bigint,
  fst_atm_dt timestamp,
  lst_atm_dt timestamp,
  atm_qty bigint,
  fst_arm_dt timestamp,
  lst_arm_dt timestamp,
  arm_qty bigint,
  fst_sberid_dt timestamp,
  lst_sberid_dt timestamp,
  sberid_qty bigint,
  tot_day_qty bigint,
  mobil_day_qty bigint,
  web_day_qty bigint,
  atm_day_qty bigint,
  arm_day_qty bigint,
  sberid_day_qty bigint,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (month_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/stg/sbol_logon_aggr_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_sbol_stg.sbol_logon_aggr_stg PARTITION(month_part)
WITH base AS (
 SELECT
   cast(epk_id as bigint) as epk_id,
   login_id,
   device_info,
   application,
   CASE
     WHEN upper(application) = 'PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID'
     ELSE channel_type
   END AS channel_type,
   tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE
     WHEN channel_type IS NOT NULL THEN channel_type
     WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI'
     WHEN upper(application) = 'PHIZ_IC' THEN 'WEB'
     WHEN upper(application) LIKE '%ATM%' THEN 'ATM'
     WHEN upper(application) = 'PHIZ_IA' THEN 'SERVICE'
     ELSE NULL
   END AS channel_type_all
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl
 WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
 UNION ALL SELECT cast(epk_id as bigint), login_id, device_info, application, CASE WHEN upper(application)='PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END, tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE WHEN channel_type IS NOT NULL THEN channel_type WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI' WHEN upper(application)='PHIZ_IC' THEN 'WEB' WHEN upper(application) LIKE '%ATM%' THEN 'ATM' WHEN upper(application)='PHIZ_IA' THEN 'SERVICE' ELSE NULL END
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl2 WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
 UNION ALL SELECT cast(epk_id as bigint), login_id, device_info, application, CASE WHEN upper(application)='PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END, tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE WHEN channel_type IS NOT NULL THEN channel_type WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI' WHEN upper(application)='PHIZ_IC' THEN 'WEB' WHEN upper(application) LIKE '%ATM%' THEN 'ATM' WHEN upper(application)='PHIZ_IA' THEN 'SERVICE' ELSE NULL END
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl3 WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
 UNION ALL SELECT cast(epk_id as bigint), login_id, device_info, application, CASE WHEN upper(application)='PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END, tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE WHEN channel_type IS NOT NULL THEN channel_type WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI' WHEN upper(application)='PHIZ_IC' THEN 'WEB' WHEN upper(application) LIKE '%ATM%' THEN 'ATM' WHEN upper(application)='PHIZ_IA' THEN 'SERVICE' ELSE NULL END
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl4 WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
 UNION ALL SELECT cast(epk_id as bigint), login_id, device_info, application, CASE WHEN upper(application)='PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END, tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE WHEN channel_type IS NOT NULL THEN channel_type WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI' WHEN upper(application)='PHIZ_IC' THEN 'WEB' WHEN upper(application) LIKE '%ATM%' THEN 'ATM' WHEN upper(application)='PHIZ_IA' THEN 'SERVICE' ELSE NULL END
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl5 WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
 UNION ALL SELECT cast(epk_id as bigint), login_id, device_info, application, CASE WHEN upper(application)='PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END, tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE WHEN channel_type IS NOT NULL THEN channel_type WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI' WHEN upper(application)='PHIZ_IC' THEN 'WEB' WHEN upper(application) LIKE '%ATM%' THEN 'ATM' WHEN upper(application)='PHIZ_IA' THEN 'SERVICE' ELSE NULL END
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl6 WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
 UNION ALL SELECT cast(epk_id as bigint), login_id, device_info, application, CASE WHEN upper(application)='PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END, tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE WHEN channel_type IS NOT NULL THEN channel_type WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI' WHEN upper(application)='PHIZ_IC' THEN 'WEB' WHEN upper(application) LIKE '%ATM%' THEN 'ATM' WHEN upper(application)='PHIZ_IA' THEN 'SERVICE' ELSE NULL END
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl7 WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
 UNION ALL SELECT cast(epk_id as bigint), login_id, device_info, application, CASE WHEN upper(application)='PHIZ_SBER_ID' THEN 'INTERNET_BANK_SBERID' ELSE channel_type END, tb_nmb, osb_nmb, vsp_nmb, schema, logon_dt, cnt_logon, day_part,
   CASE WHEN channel_type IS NOT NULL THEN channel_type WHEN upper(application) LIKE '%MOBIL%' THEN 'MAPI' WHEN upper(application)='PHIZ_IC' THEN 'WEB' WHEN upper(application) LIKE '%ATM%' THEN 'ATM' WHEN upper(application)='PHIZ_IA' THEN 'SERVICE' ELSE NULL END
 FROM custom_rb_sbol.sbol_logon_aggr_dt_ikfl_gf WHERE epk_id IS NOT NULL AND day_part >= '$startDt' AND day_part < '$endDt' AND day_part < '$firstDayCurrentMonth'
), monthly AS (
 SELECT
   coalesce(epk_id,-1) as epk_id,
   substr(day_part,1,7) as month_part,
   min(logon_dt) as fst_logon_dt,
   max(logon_dt) as lst_logon_dt,
   sum(cnt_logon) as tot_qty,
   min(case when upper(channel_type_all) in ('MAPI','MB') then logon_dt end) as fst_mobil_dt,
   max(case when upper(channel_type_all) in ('MAPI','MB') then logon_dt end) as lst_mobil_dt,
   sum(case when upper(channel_type_all) in ('MAPI','MB') then cnt_logon else 0 end) as mobil_qty,
   min(case when upper(channel_type_all) in ('WEB','CSA') then logon_dt end) as fst_web_dt,
   max(case when upper(channel_type_all) in ('WEB','CSA') then logon_dt end) as lst_web_dt,
   sum(case when upper(channel_type_all) in ('WEB','CSA') then cnt_logon else 0 end) as web_qty,
   min(case when upper(channel_type_all) in ('ATM','TERMINAL') then logon_dt end) as fst_atm_dt,
   max(case when upper(channel_type_all) in ('ATM','TERMINAL') then logon_dt end) as lst_atm_dt,
   sum(case when upper(channel_type_all) in ('ATM','TERMINAL') then cnt_logon else 0 end) as atm_qty,
   min(case when upper(channel_type_all) = 'SERVICE' then logon_dt end) as fst_arm_dt,
   max(case when upper(channel_type_all) = 'SERVICE' then logon_dt end) as lst_arm_dt,
   sum(case when upper(channel_type_all) = 'SERVICE' then cnt_logon else 0 end) as arm_qty,
   min(case when upper(channel_type_all) = 'INTERNET_BANK_SBERID' then logon_dt end) as fst_sberid_dt,
   max(case when upper(channel_type_all) = 'INTERNET_BANK_SBERID' then logon_dt end) as lst_sberid_dt,
   sum(case when upper(channel_type_all) = 'INTERNET_BANK_SBERID' then cnt_logon else 0 end) as sberid_qty,
   count(*) as tot_day_qty,
   sum(case when upper(channel_type_all) in ('MAPI','MB') then 1 else 0 end) as mobil_day_qty,
   sum(case when upper(channel_type_all) in ('WEB','CSA') then 1 else 0 end) as web_day_qty,
   sum(case when upper(channel_type_all) in ('ATM','TERMINAL') then 1 else 0 end) as atm_day_qty,
   sum(case when upper(channel_type_all) = 'SERVICE' then 1 else 0 end) as arm_day_qty,
   sum(case when upper(channel_type_all) = 'INTERNET_BANK_SBERID' then 1 else 0 end) as sberid_day_qty
 FROM base GROUP BY coalesce(epk_id,-1), substr(day_part,1,7)
)
SELECT
 epk_id, fst_logon_dt, lst_logon_dt, tot_qty, fst_mobil_dt, lst_mobil_dt, mobil_qty,
 fst_web_dt, lst_web_dt, web_qty, fst_atm_dt, lst_atm_dt, atm_qty, fst_arm_dt, lst_arm_dt,
 arm_qty, fst_sberid_dt, lst_sberid_dt, sberid_qty, tot_day_qty, mobil_day_qty, web_day_qty,
 atm_day_qty, arm_day_qty, sberid_day_qty,
 $ctlLoading as ctl_loading, current_timestamp() as ctl_validfrom, 'I' as ctl_action, month_part
FROM monthly;

coalesce_files('/data/custom/rb/sbol/stg/sbol_logon_aggr_stg','/data/custom/rb/sbol/stg/sbol_logon_aggr_temp',128,'snappy',CAST($parallelDegree as INT));
move_table_to_schema(
  s2tTableList='sbol_logon_aggr_stg->sbol_logon_aggr', srcSchema='custom_rb_sbol_stg', tgtSchema='custom_rb_sbol', compressionType='snappy', workMode='arc', truncateIncFilterList='', truncateArcFilterList='sbol_logon_aggr->all', instanceFilter='', truncateStgFromPa=true
);
let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "arc"}';
publish($meta);
