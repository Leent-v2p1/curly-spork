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
delete_hdfs_dir("/data/custom/rb/evk/stg/ref_union_campaign_channel_snp_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

DROP TABLE IF EXISTS custom_rb_evk_stg.ref_union_campaign_channel_snp_stg;
CREATE EXTERNAL TABLE custom_rb_evk_stg.ref_union_campaign_channel_snp_stg(
        channel_id	bigint,
        channel_name	string,
        channel_type	string,
        channel_name_desc	string,
        channel_system	string,
        ctl_loading	int,
        ctl_validfrom	timestamp,
        ctl_action	string)
PARTITIONED BY ( 
  `date_build_snp` string)
STORED AS PARQUET
LOCATION '/data/custom/rb/evk/stg/ref_union_campaign_channel_snp_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');
MSCK REPAIR TABLE custom_rb_evk_stg.ref_union_campaign_channel_snp_stg;

INSERT OVERWRITE TABLE custom_rb_evk_stg.ref_union_campaign_channel_snp_stg
SELECT
	cast(channel_id as bigint) as channel_id,
	cast(channel_name as string) as channel_name,
	cast(channel_type as string) as channel_type,
	cast(channel_name_desc as string) as channel_name_desc,
	cast(channel_system as string) as channel_system,
	$ctlLoading as ctl_loading,
    cast (current_date() as timestamp) as ctl_validfrom,
    "I" as ctl_action,
	cast(CURRENT_DATE as string) as date_build_snp
FROM custom_rb_evk.ref_union_campaign_channel;

coalesce_files(
	"/data/custom/rb/evk/stg/ref_union_campaign_channel_snp_stg",
	"/data/custom/rb/evk/stg/ref_union_campaign_channel_snp_temp",
	128, "snappy", CAST($parallelDegree as INT)
);

move_table_to_schema(
    s2tTableList="ref_union_campaign_channel_snp_stg->ref_union_campaign_channel_snp",
    srcSchema="custom_rb_evk_stg",
    tgtSchema="custom_rb_evk",
    compressionType="snappy",
    workMode=$typeWf,
    truncateIncFilterList="",
    truncateArcFilterList="ref_union_campaign_channel_snp->all",
    instanceFilter="",
    truncateStgFromPa=true
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);