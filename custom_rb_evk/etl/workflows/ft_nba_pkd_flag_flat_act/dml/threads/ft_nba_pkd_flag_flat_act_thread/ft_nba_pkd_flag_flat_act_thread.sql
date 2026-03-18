let typeWf = '${$type}';
print("type - $typeWf");
let parallelDegree = '${$parallel_degree}';
print("parallelDegree - $parallelDegree");
let ctlLoading = '${$loading_id}';
print("ctlLoading - $ctlLoading");

TRY
delete_hdfs_dir("/data/custom/rb/evk/stg/ft_nba_pkd_flag_flat_act_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

let buildDate = (SELECT current_timestamp())[0][0];

let businessDate = (SELECT date_format(date_add('$buildDate', -1), 'yyyy-MM-dd'))[0][0];
print("businessDate - $businessDate");



let startDt = '${$start_dt}';
if $startDt == "" then
    let startDt = (SELECT date_add(date_format('$buildDate','yyyy-MM-dd'), -2))[0][0];
end if;

print("startDt - $startDt");

let endDt = '${$end_dt}';
if $endDt == "" then
    let endDt = (SELECT date_format('$buildDate','yyyy-MM-dd'))[0][0];
end if;

print("endDt - $endDt");

DROP TABLE IF EXISTS custom_rb_evk_stg.ft_nba_pkd_flag_flat_act_stg;
CREATE EXTERNAL TABLE custom_rb_evk_stg.ft_nba_pkd_flag_flat_act_stg(
  `epk_id` bigint, 
  `cmpn_sms_avail_nflag_no_sl` int, 
  `cmpn_sms_avail_nflag` int, 
  `cmpn_push_sbol_avail_nflag_no_sl` int, 
  `cmpn_push_sbol_avail_nflag` int, 
  `cmpn_push_sms_avail_nflag_no_sl` int, 
  `cmpn_push_sms_avail_nflag` int, 
  `cmpn_tm_avail_nflag_no_sl` int, 
  `cmpn_tm_avail_nflag` int, 
  `cmpn_tm_robot_avail_nflag_no_sl` int, 
  `cmpn_tm_robot_avail_nflag` int, 
  `cmpn_push_sberinvestor_avail_nflag_no_sl` int, 
  `cmpn_push_sberinvestor_avail_nflag` int, 
  `cmpn_email_avail_nflag_no_sl` int, 
  `cmpn_email_avail_nflag` int, 
  `cmpn_channel_erkc_ivr_in_avail_nflag_no_sl` int, 
  `cmpn_channel_erkc_ivr_in_avail_nflag` int, 
  `cmpn_channel_sitesber_avail_nflag_no_sl` int, 
  `cmpn_channel_sitesber_avail_nflag` int, 
  `cmpn_channel_sbolpro_avail_nflag_no_sl` int, 
  `cmpn_channel_sbolpro_avail_nflag` int, 
  `cmpn_channel_kmpremierpull_avail_nflag_no_sl` int, 
  `cmpn_channel_kmpremierpull_avail_nflag` int, 
  `cmpn_banner_mpsbol_avail_nflag_no_sl` int, 
  `cmpn_banner_mpsbol_avail_nflag` int, 
  `cmpn_banner_sberinvestor_avail_nflag_no_sl` int, 
  `cmpn_banner_sberinvestor_avail_nflag` int, 
  `cmpn_banner_websbol_avail_nflag_no_sl` int, 
  `cmpn_banner_websbol_avail_nflag` int, 
  `cmpn_dsa_avail_nflag_no_sl` int, 
  `cmpn_dsa_avail_nflag` int, 
  `cmpn_dkm_avail_nflag_no_sl` int, 
  `cmpn_dkm_avail_nflag` int, 
  `cmpn_sberads_avail_nflag_no_sl` int, 
  `cmpn_sberads_avail_nflag` int, 
  `cmpn_va_avail_nflag_no_sl` int, 
  `cmpn_va_avail_nflag` int, 
  `cmpn_push_km_sb1_avail_nflag_no_sl` int, 
  `cmpn_push_km_sb1_avail_nflag` int, 
  `cmpn_channel_kmsb1pull_avail_nflag_no_sl` int, 
  `cmpn_channel_kmsb1pull_avail_nflag` int, 
  `cmpn_atm_avail_nflag` int, 
  `cmpn_atm_avail_nflag_no_sl` int, 
  `cmpn_km_b2b_avail_nflag` int, 
  `cmpn_km_b2b_avail_nflag_no_sl` int, 
  `cmpn_push_km_pb_avail_nflag` int, 
  `cmpn_push_km_pb_avail_nflag_no_sl` int, 
  `cmpn_push_km_premier_avail_nflag` int, 
  `cmpn_push_km_premier_avail_nflag_no_sl` int, 
  `cmpn_sb_bol_avail_nflag` int, 
  `cmpn_sb_bol_avail_nflag_no_sl` int, 
  `cmpn_sber_id_avail_nflag` int, 
  `cmpn_sber_id_avail_nflag_no_sl` int, 
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
PARTITIONED BY ( 
  `report_dt` string)
STORED AS PARQUET
LOCATION '/data/custom/rb/evk/stg/ft_nba_pkd_flag_flat_act_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');
MSCK REPAIR TABLE custom_rb_evk_stg.ft_nba_pkd_flag_flat_act_stg;

INSERT OVERWRITE TABLE custom_rb_evk_stg.ft_nba_pkd_flag_flat_act_stg PARTITION(report_dt)
SELECT
	epk_id,
	cast(cmpn_sms_avail_nflag_no_sl as int) as cmpn_sms_avail_nflag_no_sl,
	cast(cmpn_sms_avail_nflag as int) as cmpn_sms_avail_nflag,
	cast(cmpn_push_sbol_avail_nflag_no_sl as int) as cmpn_push_sbol_avail_nflag_no_sl,
	cast(cmpn_push_sbol_avail_nflag as int) as cmpn_push_sbol_avail_nflag,
	cast(cmpn_push_sms_avail_nflag_no_sl as int) as cmpn_push_sms_avail_nflag_no_sl,
	cast(cmpn_push_sms_avail_nflag as int) as cmpn_push_sms_avail_nflag,
	cast(cmpn_tm_avail_nflag_no_sl as int) as cmpn_tm_avail_nflag_no_sl,
	cast(cmpn_tm_avail_nflag as int) as cmpn_tm_avail_nflag,
	cast(cmpn_tm_robot_avail_nflag_no_sl as int) as cmpn_tm_robot_avail_nflag_no_sl,
	cast(cmpn_tm_robot_avail_nflag as int) as cmpn_tm_robot_avail_nflag,
	cast(cmpn_push_sberinvestor_avail_nflag_no_sl as int) as cmpn_push_sberinvestor_avail_nflag_no_sl,
	cast(cmpn_push_sberinvestor_avail_nflag as int) as cmpn_push_sberinvestor_avail_nflag,
	cast(cmpn_email_avail_nflag_no_sl as int) as cmpn_email_avail_nflag_no_sl,
	cast(cmpn_email_avail_nflag as int) as cmpn_email_avail_nflag,
	cast(cmpn_channel_erkc_ivr_in_avail_nflag_no_sl as int) as cmpn_channel_erkc_ivr_in_avail_nflag_no_sl,
	cast(cmpn_channel_erkc_ivr_in_avail_nflag as int) as cmpn_channel_erkc_ivr_in_avail_nflag,
	cast(cmpn_channel_sitesber_avail_nflag_no_sl as int) as cmpn_channel_sitesber_avail_nflag_no_sl,
	cast(cmpn_channel_sitesber_avail_nflag as int) as cmpn_channel_sitesber_avail_nflag,
	cast(cmpn_channel_sbolpro_avail_nflag_no_sl as int) as cmpn_channel_sbolpro_avail_nflag_no_sl,
	cast(cmpn_channel_sbolpro_avail_nflag as int) as cmpn_channel_sbolpro_avail_nflag,
	cast(cmpn_channel_kmpremierpull_avail_nflag_no_sl as int) as cmpn_channel_kmpremierpull_avail_nflag_no_sl,
	cast(cmpn_channel_kmpremierpull_avail_nflag as int) as cmpn_channel_kmpremierpull_avail_nflag,
	cast(cmpn_banner_mpsbol_avail_nflag_no_sl as int) as cmpn_banner_mpsbol_avail_nflag_no_sl,
	cast(cmpn_banner_mpsbol_avail_nflag as int) as cmpn_banner_mpsbol_avail_nflag,
	cast(cmpn_banner_sberinvestor_avail_nflag_no_sl as int) as cmpn_banner_sberinvestor_avail_nflag_no_sl,
	cast(cmpn_banner_sberinvestor_avail_nflag as int) as cmpn_banner_sberinvestor_avail_nflag,
	cast(cmpn_banner_websbol_avail_nflag_no_sl as int) as cmpn_banner_websbol_avail_nflag_no_sl,
	cast(cmpn_banner_websbol_avail_nflag as int) as cmpn_banner_websbol_avail_nflag,
	cast(cmpn_dsa_avail_nflag_no_sl as int) as cmpn_dsa_avail_nflag_no_sl,
	cast(cmpn_dsa_avail_nflag as int) as cmpn_dsa_avail_nflag,
	cast(cmpn_dkm_avail_nflag_no_sl as int) as cmpn_dkm_avail_nflag_no_sl,
	cast(cmpn_dkm_avail_nflag as int) as cmpn_dkm_avail_nflag,
	cast(cmpn_sberads_avail_nflag_no_sl as int) as cmpn_sberads_avail_nflag_no_sl,
	cast(cmpn_sberads_avail_nflag as int) as cmpn_sberads_avail_nflag,
	cast(cmpn_va_avail_nflag_no_sl as int) as cmpn_va_avail_nflag_no_sl,
	cast(cmpn_va_avail_nflag as int) as cmpn_va_avail_nflag,
	cast(cmpn_push_km_sb1_avail_nflag_no_sl as int) as cmpn_push_km_sb1_avail_nflag_no_sl,
	cast(cmpn_push_km_sb1_avail_nflag as int) as cmpn_push_km_sb1_avail_nflag,
	cast(cmpn_channel_kmsb1pull_avail_nflag_no_sl as int) as cmpn_channel_kmsb1pull_avail_nflag_no_sl,
	cast(cmpn_channel_kmsb1pull_avail_nflag as int) as cmpn_channel_kmsb1pull_avail_nflag,
	cast(cmpn_atm_avail_nflag as int) as cmpn_atm_avail_nflag,
	cast(cmpn_atm_avail_nflag_no_sl as int) as cmpn_atm_avail_nflag_no_sl,
	cast(cmpn_km_b2b_avail_nflag as int) as cmpn_km_b2b_avail_nflag,
	cast(cmpn_km_b2b_avail_nflag_no_sl as int) as cmpn_km_b2b_avail_nflag_no_sl,
	cast(cmpn_push_km_pb_avail_nflag as int) as cmpn_push_km_pb_avail_nflag,
	cast(cmpn_push_km_pb_avail_nflag_no_sl as int) as cmpn_push_km_pb_avail_nflag_no_sl,
	cast(cmpn_push_km_premier_avail_nflag as int) as cmpn_push_km_premier_avail_nflag,
	cast(cmpn_push_km_premier_avail_nflag_no_sl as int) as cmpn_push_km_premier_avail_nflag_no_sl,
	cast(cmpn_sb_bol_avail_nflag as int) as cmpn_sb_bol_avail_nflag,
	cast(cmpn_sb_bol_avail_nflag_no_sl as int) as cmpn_sb_bol_avail_nflag_no_sl,
	cast(cmpn_sber_id_avail_nflag as int) as cmpn_sber_id_avail_nflag,
	cast(cmpn_sber_id_avail_nflag_no_sl as int) as cmpn_sber_id_avail_nflag_no_sl,
	$ctlLoading as ctl_loading,
	cast (current_date() as timestamp) as ctl_validfrom,
    "I" as ctl_action,
	cast(report_dt as string) as report_dt
FROM ${$src_schema_name}.ft_nba_pkd_flag_flat_act
where 1=1;

coalesce_files(
	"/data/custom/rb/evk/stg/ft_nba_pkd_flag_flat_act_stg",
	"/data/custom/rb/evk/stg/ft_nba_pkd_flag_flat_act_temp",
	128, "snappy", CAST($parallelDegree as INT)
);

move_table_to_schema(
    s2tTableList="ft_nba_pkd_flag_flat_act_stg->ft_nba_pkd_flag_flat_act",
    srcSchema="custom_rb_evk_stg",
    tgtSchema="custom_rb_evk",
    compressionType="snappy",
    workMode=$typeWf,
    truncateIncFilterList="",
    truncateArcFilterList="ft_nba_pkd_flag_flat_act->all",
    instanceFilter="",
    truncateStgFromPa=true
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);