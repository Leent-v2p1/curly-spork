let typeWf = '${$type}';
print("type - $typeWf");
let parallelDegree = '${$parallel_degree}';
print("parallelDegree - $parallelDegree");
let ctlLoading = '${$loading_id}';
print("ctlLoading - $ctlLoading");
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("businessDate - $businessDate");

let startDt = '${$start_dt}';
let endDt = '${$end_dt}';

print("startDt - $startDt");
print("endDt - $endDt");

if $startDt == "" then
    let startDt = (SELECT date_add(CURRENT_DATE, -7))[0][0];
end if

if $endDt == "" then
    let endDt = (SELECT CURRENT_DATE)[0][0];
end if

print("startDt - $startDt");
print("endDt - $endDt");

TRY
delete_hdfs_dir("/data/custom/rb/vnv/stg/dm_customer_engagement_snapshot_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

DROP TABLE IF EXISTS custom_rb_vnv_stg.dm_customer_engagement_snapshot_stg;
CREATE EXTERNAL TABLE custom_rb_vnv_stg.dm_customer_engagement_snapshot_stg(
    customer_id bigint,
    customer_status string,
    segment_code string,
    segment_name string,
    region_name string,
    city_name string,
    preferred_channel string,
    last_contact_dt string,
    last_purchase_dt string,
    active_product_cnt int,
    avg_monthly_turnover decimal(18,2),
    loyalty_score int,
    churn_probability decimal(5,4),
    campaign_eligible_flg int,
    ctl_loading int,
    ctl_validfrom timestamp,
    ctl_action string
)
PARTITIONED BY (snapshot_dt string)
STORED AS PARQUET
LOCATION
'/data/custom/rb/vnv/stg/dm_customer_engagement_snapshot_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');
MSCK REPAIR TABLE custom_rb_vnv_stg.dm_customer_engagement_snapshot_stg;

if ($typeWf == "arc") then
    INSERT OVERWRITE TABLE custom_rb_vnv_stg.dm_customer_engagement_snapshot_stg PARTITION(snapshot_dt)
    SELECT
      cast(customer_id as bigint) as customer_id,
      cast(customer_status as string) as customer_status,
      cast(segment_code as string) as segment_code,
      cast(segment_name as string) as segment_name,
      cast(region_name as string) as region_name,
      cast(city_name as string) as city_name,
      cast(preferred_channel as string) as preferred_channel,
      cast(last_contact_dt as string) as last_contact_dt,
      cast(last_purchase_dt as string) as last_purchase_dt,
      cast(active_product_cnt as int) as active_product_cnt,
      cast(avg_monthly_turnover as decimal(18,2)) as avg_monthly_turnover,
      cast(loyalty_score as int) as loyalty_score,
      cast(churn_probability as decimal(5,4)) as churn_probability,
      cast(campaign_eligible_flg as int) as campaign_eligible_flg,
      $ctlLoading as ctl_loading,
      current_timestamp() as ctl_validfrom,
      "I" as ctl_action,
      cast(snapshot_dt as string) as snapshot_dt
    from ${$src_schema_name}.dm_customer_engagement_snapshot_hdp;
end if

if ($typeWf == "inc") then
    INSERT OVERWRITE TABLE custom_rb_vnv_stg.dm_customer_engagement_snapshot_stg PARTITION(snapshot_dt)
    SELECT
      cast(customer_id as bigint) as customer_id,
      cast(customer_status as string) as customer_status,
      cast(segment_code as string) as segment_code,
      cast(segment_name as string) as segment_name,
      cast(region_name as string) as region_name,
      cast(city_name as string) as city_name,
      cast(preferred_channel as string) as preferred_channel,
      cast(last_contact_dt as string) as last_contact_dt,
      cast(last_purchase_dt as string) as last_purchase_dt,
      cast(active_product_cnt as int) as active_product_cnt,
      cast(avg_monthly_turnover as decimal(18,2)) as avg_monthly_turnover,
      cast(loyalty_score as int) as loyalty_score,
      cast(churn_probability as decimal(5,4)) as churn_probability,
      cast(campaign_eligible_flg as int) as campaign_eligible_flg,
      $ctlLoading as ctl_loading,
      current_timestamp() as ctl_validfrom,
      "I" as ctl_action,
      cast(snapshot_dt as string) as snapshot_dt
    from ${$src_schema_name}.dm_customer_engagement_snapshot_hdp
    where snapshot_dt >= to_date('$startDt','yyyy-MM-dd') and snapshot_dt <= to_date('$endDt','yyyy-MM-dd');
end if

coalesce_files(
    "/data/custom/rb/vnv/stg/dm_customer_engagement_snapshot_stg",
    "/data/custom/rb/vnv/stg/dm_customer_engagement_snapshot_temp",
    128, "snappy", CAST($parallelDegree as INT)
);

move_table_to_schema(
    s2tTableList="dm_customer_engagement_snapshot_stg->dm_customer_engagement_snapshot",
    srcSchema="custom_rb_vnv_stg",
    tgtSchema="custom_rb_vnv",
    compressionType="snappy",
    workMode=$typeWf,
    truncateIncFilterList="",
    truncateArcFilterList="dm_customer_engagement_snapshot->all",
    instanceFilter="",
    truncateStgFromPa=true
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);
