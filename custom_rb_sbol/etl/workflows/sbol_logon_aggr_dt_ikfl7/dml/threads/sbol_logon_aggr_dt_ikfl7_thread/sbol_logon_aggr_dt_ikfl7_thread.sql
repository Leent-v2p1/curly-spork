let typeWf = '${$type}';
let parallelDegree = '${$parallel_degree}';
let ctlLoading = '${$loading_id}';
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("type - $typeWf");
print("parallelDegree - $parallelDegree");
print("ctlLoading - $ctlLoading");
print("businessDate - $businessDate");

let migrationDate = '2024-08-01';
let startDt = '${$start_dt}';
let endDt = '${$end_dt}';
if $startDt == '' then
    let startDt = '2015-01-01';
end if;
if $endDt == '' then
    let endDt = (SELECT date_format(current_timestamp(), 'yyyy-MM-dd'))[0][0];
end if;
print("startDt - $startDt");
print("endDt - $endDt");

TRY
delete_hdfs_dir('/data/custom/rb/sbol/stg/sbol_logon_aggr_dt_ikfl7_stg');
CATCH ex THEN
    print($ex['message']);
END

DROP TABLE IF EXISTS custom_rb_sbol_stg.sbol_logon_aggr_dt_ikfl7_stg;
CREATE EXTERNAL TABLE custom_rb_sbol_stg.sbol_logon_aggr_dt_ikfl7_stg(
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/stg/sbol_logon_aggr_dt_ikfl7_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_sbol_stg.sbol_logon_aggr_dt_ikfl7_stg PARTITION(day_part)
WITH logins AS (
  SELECT id, last_logon_card_tb AS tb_nmb, last_logon_card_osb AS osb_nmb, last_logon_card_vsp AS vsp_nmb
  FROM ${$src_schema_name}.sbol_logon_ikfl7
),
logon_base AS (
  SELECT
    cast(l.login_id as decimal(38,0)) as login_id,
    cast(l.application as string) as application,
    cast(l.channel_type as string) as channel_type,
    cast(l.logon_dt as timestamp) as logon_dt,
    cast(l.epk_id as bigint) as epk_id,
    cast(l.device_info as string) as device_info,
    cast(g.tb_nmb as string) as tb_nmb,
    cast(g.osb_nmb as string) as osb_nmb,
    cast(g.vsp_nmb as string) as vsp_nmb,
    cast(l.day_part as string) as day_part
  FROM ${$src_schema_name}.sbol_logon_ikfl7 l
  LEFT JOIN logins g ON cast(l.login_id as decimal(38,0)) = cast(g.id as decimal(38,0))
  WHERE cast(l.logon_dt as timestamp) >= to_timestamp('$startDt')
    AND cast(l.logon_dt as timestamp) < to_timestamp('$endDt')
    AND cast(l.logon_dt as timestamp) < date_trunc('DAY', current_timestamp())
    AND cast(l.day_part as string) >= '$startDt'
    AND (cast(l.day_part as string) < '$migrationDate' OR (cast(l.day_part as string) >= '$migrationDate' AND upper(coalesce(l.channel_type,'')) IN ('MAPI','WEB','SBOL')))
),
launcher_src AS (
  SELECT
    cast(logon_id as decimal(38,0)) as login_id,
    cast(null as string) as application,
    cast(channel_type as string) as channel_type,
    cast(logon_dt as timestamp) as logon_dt,
    cast(epk_id as bigint) as epk_id,
    cast(device_info as string) as device_info,
    cast(tb_nmb as string) as tb_nmb,
    cast(osb_nmb as string) as osb_nmb,
    cast(vsp_nmb as string) as vsp_nmb,
    cast(substr(logon_dt,1,10) as string) as day_part
  FROM (
      SELECT
          conv(substr(md5(id), -15, 15), 16, 10) as id_hash,
          logon_id, logon_dt, epk_id, device_info, channel_type, day_part,
          tb_nmb, osb_nmb, vsp_nmb, id_instance
      FROM ${$src_schema_name}.sbol_logon_launcher
      WHERE upper(type) = 'LOGON'
        AND (lower(cast(id_instance as string)) = '7' OR (cast(coalesce(cast(id_instance as int),0) as int)=0 AND '7'='gf'))
  ) x
  WHERE cast(day_part as string) >= '$migrationDate'
    AND cast(day_part as string) >= '$startDt'
    AND cast(logon_dt as timestamp) >= to_timestamp('$startDt')
    AND cast(logon_dt as timestamp) < to_timestamp('$endDt')
    AND cast(logon_dt as timestamp) < date_trunc('DAY', current_timestamp())
),
filtered AS (
  SELECT * FROM logon_base
  UNION ALL
  SELECT * FROM launcher_src
),
aggr AS (
  SELECT
    login_id,
    application,
    channel_type,
    cast(date_trunc('DAY', logon_dt) as timestamp) as logon_dt,
    coalesce(cast(epk_id as bigint), -1) as epk_id,
    device_info,
    min(logon_dt) as min_logon_dt,
    max(logon_dt) as max_logon_dt,
    count(*) as cnt_logon,
    max(tb_nmb) as tb_nmb,
    max(osb_nmb) as osb_nmb,
    max(vsp_nmb) as vsp_nmb,
    date_format(date_trunc('DAY', logon_dt), 'yyyy-MM-dd') as day_part
  FROM filtered
  GROUP BY login_id, application, channel_type, cast(date_trunc('DAY', logon_dt) as timestamp), device_info, coalesce(cast(epk_id as bigint), -1)
)
SELECT
  login_id,
  application,
  channel_type,
  logon_dt,
  epk_id,
  device_info,
  min_logon_dt,
  max_logon_dt,
  cnt_logon,
  tb_nmb,
  osb_nmb,
  vsp_nmb,
  '7' as schema,
  $ctlLoading as ctl_loading,
  current_timestamp() as ctl_validfrom,
  'I' as ctl_action,
  day_part
FROM aggr;

coalesce_files('/data/custom/rb/sbol/stg/sbol_logon_aggr_dt_ikfl7_stg','/data/custom/rb/sbol/stg/sbol_logon_aggr_dt_ikfl7_temp',128,'snappy',CAST($parallelDegree as INT));

let partTruncList = '';
for part_name in (show partitions custom_rb_sbol.sbol_logon_aggr_dt_ikfl7) loop
    let curr_part = regexp_extract($part_name[0], 'day_part=(\d\d\d\d-\d\d-\d\d)');
    if $curr_part >= $startDt and $curr_part < $endDt then
        if $partTruncList == '' then
            let partTruncList = $part_name[0];
        else
            let partTruncList = concat_ws(',', $partTruncList, $part_name[0]);
        end if;
    end if;
end loop;

move_table_to_schema(
    s2tTableList='sbol_logon_aggr_dt_ikfl7_stg->sbol_logon_aggr_dt_ikfl7',
    srcSchema='custom_rb_sbol_stg',
    tgtSchema='custom_rb_sbol',
    compressionType='snappy',
    workMode='inc',
    truncateIncFilterList='',
    truncateArcFilterList='sbol_logon_aggr_dt_ikfl7->all',
    instanceFilter='',
    truncateStgFromPa=true
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "inc"}';
publish($meta);

-- NOTE: existing partitions in custom_rb_sbol.sbol_logon_aggr_dt_ikfl7 for [$startDt, $endDt) should be truncated by scheduler/runtime if required.
