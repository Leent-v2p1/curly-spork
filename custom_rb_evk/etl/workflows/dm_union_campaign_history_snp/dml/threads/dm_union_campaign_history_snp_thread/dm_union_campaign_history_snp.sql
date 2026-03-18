let typeWf = '${$type}';
print("type - $typeWf");
let parallelDegree = '${$parallel_degree}';
print("parallelDegree - $parallelDegree");
let ctlLoading = '${$loading_id}';
print("ctlLoading - $ctlLoading");

let deep_monthly = '${$deep_monthly}';
let DEFAULT_DEPTH_MONTHLY= 6;

if $typeWf == "arc" then
    if $deep_monthly == "" then
        let deep_monthly = $DEFAULT_DEPTH_MONTHLY;
    end if;
else
    let deep_monthly = 1;
end if;


let buildDate = (SELECT current_timestamp())[0][0];


let businessDate = (SELECT date_format(date_add('$buildDate', -1), 'yyyy-MM-dd'))[0][0];
print("businessDate - $businessDate");


let startDt = '${$start_dt}';
if $startDt == "" then
    let startDt = (SELECT date_format(add_months(date_format('$buildDate','yyyy-MM-dd'), -$deep_monthly),'yyyy-MM-01'))[0][0];
end if;

print("startDt - $startDt");

let endDt = '${$end_dt}';
if $endDt == "" then
    let endDt = (SELECT date_format('$buildDate','yyyy-MM-dd'))[0][0];
end if;

print("endDt - $endDt");

TRY
delete_hdfs_dir("/data/custom/rb/evk/stg/dm_union_campaign_history_snp_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

DROP TABLE IF EXISTS custom_rb_evk_stg.dm_union_campaign_history_snp_stg;
CREATE EXTERNAL TABLE custom_rb_evk_stg.dm_union_campaign_history_snp_stg(
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
  `ctl_action` string
)
PARTITIONED BY (
  `date_build_snp` string,
  `start_dt` string)
STORED AS PARQUET
LOCATION '/data/custom/rb/evk/stg/dm_union_campaign_history_snp_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');
MSCK REPAIR TABLE custom_rb_evk_stg.dm_union_campaign_history_snp_stg;

INSERT OVERWRITE TABLE custom_rb_evk_stg.dm_union_campaign_history_snp_stg PARTITION(date_build_snp, start_dt)
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
                cast(CURRENT_DATE as string) as date_build_snp,
                cast(start_dt as string) as start_dt
FROM custom_rb_evk.dm_union_campaign_history
where start_dt >= '$startDt' and end_dt < '$endDt';

coalesce_files(
                "/data/custom/rb/evk/stg/dm_union_campaign_history_snp_stg",
                "/data/custom/rb/evk/stg/dm_union_campaign_history_snp_temp",
                128, "snappy", CAST($parallelDegree as INT)
);

let lower_part_bound = (SELECT date_format(add_months('$buildDate', -6), 'yyyy-MM-01'))[0][0];
let lower_part_bound_split = split($lower_part_bound, '-');

let lower_part_bound_split_year = cast($lower_part_bound_split[0] as int);
let lower_part_bound_split_month = cast($lower_part_bound_split[1] as int);
let lower_part_bound_split_day = cast($lower_part_bound_split[2] as int);

let partTruncList = "";

for part_name in (show partitions custom_rb_evk.dm_union_campaign_history_snp) loop

                let start_dt_part =  regexp_extract($part_name[0], 'start_dt=(\\d\\d\\d\\d-\\d\\d-\\d\\d)');

                let start_dt_part_split = split($start_dt_part, '-');

                let start_dt_part_split_year = cast($start_dt_part_split[0] as int);
                let start_dt_part_split_month = cast($start_dt_part_split[1] as int);
                let start_dt_part_split_day = cast($start_dt_part_split[2] as int);

                if ($lower_part_bound_split_year > $start_dt_part_split_year) or (($lower_part_bound_split_year == $start_dt_part_split_year) and ($lower_part_bound_split_month > $start_dt_part_split_month))
                then
                               if $partTruncList == ""
                               then
                                               let partTruncList = $part_name[0];
                               else
                                               let partTruncList = concat_ws(',', $partTruncList, $part_name[0]);
                               end if;
                end if;

end loop;

print($partTruncList);

if $partTruncList == ""
then
                move_table_to_schema(
                               s2tTableList="dm_union_campaign_history_snp_stg->dm_union_campaign_history_snp",
                               srcSchema="custom_rb_evk_stg",
                               tgtSchema="custom_rb_evk",
                               compressionType="snappy",
                               workMode=$typeWf,
                               truncateIncFilterList="",
                               truncateArcFilterList="dm_union_campaign_history_snp->all",
                               instanceFilter="",
                               truncateStgFromPa=true
                );
else
                move_table_to_schema(
                               s2tTableList="dm_union_campaign_history_snp_stg->dm_union_campaign_history_snp",
                               srcSchema="custom_rb_evk_stg",
                               tgtSchema="custom_rb_evk",
                               compressionType="snappy",
                               workMode=$typeWf,
                               truncateIncFilterList="dm_union_campaign_history_snp->$partTruncList",
                               truncateArcFilterList="dm_union_campaign_history_snp->all",
                               instanceFilter="",
                               truncateStgFromPa=true
                );
end if;

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);
