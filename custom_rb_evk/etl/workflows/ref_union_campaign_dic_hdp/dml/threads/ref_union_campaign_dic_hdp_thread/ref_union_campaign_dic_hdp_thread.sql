let typeWf = '${$type}';
print("type - $typeWf");
let parallelDegree = '${$parallel_degree}';
print("parallelDegree - $parallelDegree");
let ctlLoading = '${$loading_id}';
print("ctlLoading - $ctlLoading");

let buildDate = (SELECT current_timestamp())[0][0];

let businessDate = (SELECT date_format(date_add('$buildDate', -1), 'yyyy-MM-dd'))[0][0];
print("businessDate - $businessDate");

TRY
delete_hdfs_dir("/data/custom/rb/evk/stg/ref_union_campaign_dic_hdp_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

DROP TABLE IF EXISTS custom_rb_evk_stg.ref_union_campaign_dic_hdp_stg;
CREATE EXTERNAL TABLE custom_rb_evk_stg.ref_union_campaign_dic_hdp_stg(
  `sk_id` bigint, 
  `ab_test_id` bigint, 
  `product_id` string, 
  `product_name` string, 
  `product_group_name` string, 
  `product_sgroup_name` string, 
  `start_dt` string, 
  `end_dt` string, 
  `wave_launch_dt` string, 
  `wave_deactivation_dt` string, 
  `unique_segment` string, 
  `segment` string, 
  `campaign_code` string, 
  `campaign_name` string, 
  `campaign_name_cf` string, 
  `campaign_type` string, 
  `rnk` int, 
  `campaign_id` string, 
  `segment_id` string, 
  `wave_id` string, 
  `lal_id` string, 
  `camp_source` string, 
  `message_id` string, 
  `template_text` string, 
  `channel_id` int, 
  `channel_name` string, 
  `product_group_id` string, 
  `tg` string, 
  `template_comment` string, 
  `template_title` string, 
  `channel_name_crm` string, 
  `offer_id` string, 
  `team_id` string, 
  `channel_type` string, 
  `channel_name_rep` string, 
  `channel_system` string, 
  `camp_source_process` string, 
  `auto_launch_flg` int, 
  `campaign_trgt_type_name` string, 
  `template_text_full` string, 
  `template_name` string, 
  `params_txt` string, 
  `team_name` string, 
  `is_manual` int, 
  `load_ts` timestamp, 
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
STORED AS PARQUET
LOCATION '/data/custom/rb/evk/stg/ref_union_campaign_dic_hdp_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');

INSERT OVERWRITE TABLE custom_rb_evk_stg.ref_union_campaign_dic_hdp_stg
SELECT
	sk_id,
    ab_test_id,
    product_id,
    product_name,
    product_group_name,
    product_sgroup_name,
    cast(start_dt as string) as start_dt,
    cast(end_dt as string) as end_dt,
    cast(wave_launch_dt as string) as wave_launch_dt,
    cast(wave_deactivation_dt as string) as wave_deactivation_dt,
    unique_segment,
    segment,
    campaign_code,
    campaign_name,
    campaign_name_cf,
    campaign_type,
    rnk,
    campaign_id,
    segment_id,
    wave_id,
    lal_id,
    camp_source,
    message_id,
    template_text,
    cast(channel_id as int) as channel_id,
    channel_name,
    product_group_id,
    tg,
    template_comment,
    template_title,
    channel_name_crm,
    offer_id,
    team_id,
    channel_type,
    channel_name_rep,
    channel_system,
    camp_source_process,
    cast(auto_launch_flg as int) as auto_launch_flg,
    campaign_trgt_type_name,
    template_text_full,
    template_name,
    params_txt,
    team_name,
    is_manual,
    load_ts,
	$ctlLoading as ctl_loading,
    cast (current_date() as timestamp) as ctl_validfrom,
    "I" as ctl_action
FROM ${$src_schema_name}.ref_union_campaign_dic_hdp;

coalesce_files(
	"/data/custom/rb/evk/stg/ref_union_campaign_dic_hdp_stg",
	"/data/custom/rb/evk/stg/ref_union_campaign_dic_hdp_temp",
	128, "snappy", CAST($parallelDegree as INT)
);

move_table_to_schema(
    s2tTableList="ref_union_campaign_dic_hdp_stg->ref_union_campaign_dic_hdp",
    srcSchema="custom_rb_evk_stg",
    tgtSchema="custom_rb_evk",
    compressionType="snappy",
    workMode=$typeWf,
    truncateIncFilterList="",
    truncateArcFilterList="ref_union_campaign_dic_hdp->all",
    instanceFilter="",
    truncateStgFromPa=false
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);