let typeWf = '${$type}';
print("type - $typeWf");
let parallelDegree = '${$parallel_degree}';
print("parallelDegree - $parallelDegree");
let ctlLoading = '${$loading_id}';
print("ctlLoading - $ctlLoading");

TRY
delete_hdfs_dir("/data/custom/rb/evk/stg/dm_union_campaign_history_hdp_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

let buildDate = (SELECT date_format(current_timestamp(), 'yyyy-MM-dd'))[0][0];


let businessDate = (SELECT date_format(date_add('$buildDate', -1), 'yyyy-MM-dd'))[0][0];
print("businessDate - $businessDate");


let startDt = '${$start_dt}';

if $startDt == "" then
    let startDt = (SELECT add_months(date_format('$buildDate', 'yyyy-MM-dd'), -1))[0][0];
end if;

print("startDt - $startDt");


DROP TABLE IF EXISTS custom_rb_evk_stg.dm_union_campaign_history_hdp_stg;
CREATE EXTERNAL TABLE custom_rb_evk_stg.dm_union_campaign_history_hdp_stg(
	  `sk_id` bigint, 
	  `epk_id` bigint, 
	  `channel_id` int, 
	  `end_dt` string, 
	  `member_id` string, 
	  `member_code` string, 
	  `cg_flg` int, 
	  `contact_ts` timestamp, 
	  `delivery_ts` timestamp, 
	  `open_ts` timestamp, 
	  `started_ts` timestamp, 
	  `initial_ts` timestamp, 
	  `sub_channel` string, 
	  `load_ts` timestamp, 
	  `cg_stat_flg` int, 
	  `params_txt` string, 
	  `ctl_loading` int, 
	  `ctl_validfrom` timestamp, 
	  `ctl_action` string)
	PARTITIONED BY ( 
	  `start_dt` string)
STORED AS PARQUET
LOCATION '/data/custom/rb/evk/stg/dm_union_campaign_history_hdp_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_evk_stg.dm_union_campaign_history_hdp_stg
SELECT
	sk_id,
    epk_id,
    cast(channel_id as int) as channel_id,
    cast(end_dt as string) as end_dt,
    member_id,
    member_code,
    cast(cg_flg as int) as cg_flg,
    contact_ts,
    delivery_ts,
    open_ts,
    started_ts,
    initial_ts,
    sub_channel,
    load_ts,
    cast(cg_stat_flg as int) as cg_stat_flg,
    params_txt,
	$ctlLoading as ctl_loading,
    cast (current_date() as timestamp) as ctl_validfrom,
    "I" as ctl_action,
	cast(start_dt as string) as start_dt
FROM ${$src_schema_name}.dm_union_campaign_history_hdp
--where start_dt >= date_format(add_months('$startDt', -1), 'yyyy-MM-01')
UNION ALL
SELECT
	sk_id,
    epk_id,
    cast(channel_id as int) as channel_id,
    cast(end_dt as string) as end_dt,
    member_id,
    member_code,
    cast(cg_flg as int) as cg_flg,
    contact_ts,
    delivery_ts,
    open_ts,
    started_ts,
    initial_ts,
    sub_channel,
    load_ts,
    cast(cg_stat_flg as int) as cg_stat_flg,
    params_txt,
	$ctlLoading as ctl_loading,
    cast (current_date() as timestamp) as ctl_validfrom,
    "I" as ctl_action,
	cast(start_dt as string) as start_dt
FROM ${$src_schema_name}.dm_union_campaign_history_hdp_1
--where start_dt >= date_format(add_months('$startDt', -1), 'yyyy-MM-01')
UNION ALL
SELECT
	sk_id,
    epk_id,
    cast(channel_id as int) as channel_id,
    cast(end_dt as string) as end_dt,
    member_id,
    member_code,
    cast(cg_flg as int) as cg_flg,
    contact_ts,
    delivery_ts,
    open_ts,
    started_ts,
    initial_ts,
    sub_channel,
    load_ts,
    cast(cg_stat_flg as int) as cg_stat_flg,
    params_txt,
	$ctlLoading as ctl_loading,
    cast (current_date() as timestamp) as ctl_validfrom,
    "I" as ctl_action,
	cast(start_dt as string) as start_dt
FROM ${$src_schema_name}.dm_union_campaign_history_hdp_2
--where start_dt >= date_format(add_months('$startDt', -1), 'yyyy-MM-01')
;

coalesce_files(
	"/data/custom/rb/evk/stg/dm_union_campaign_history_hdp_stg",
	"/data/custom/rb/evk/stg/dm_union_campaign_history_hdp_temp",
	128, "snappy", CAST($parallelDegree as INT)
);

move_table_to_schema(
    s2tTableList="dm_union_campaign_history_hdp_stg->dm_union_campaign_history",
    srcSchema="custom_rb_evk_stg",
    tgtSchema="custom_rb_evk",
    compressionType="snappy",
    workMode=$typeWf,
    truncateIncFilterList="",
    truncateArcFilterList="dm_union_campaign_history->all",
    instanceFilter="",
    truncateStgFromPa=true
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];

print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);