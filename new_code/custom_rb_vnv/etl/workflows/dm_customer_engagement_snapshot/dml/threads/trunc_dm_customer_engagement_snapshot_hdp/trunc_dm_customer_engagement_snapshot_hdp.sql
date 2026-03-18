let path_dir = "/data/custom/rb/greenplum/stg/dm_customer_engagement_snapshot_hdp";
let is_dev = '${$is_dev}';

if ($is_dev == 'y') then
    let path_dir = "/data/custom/rb/vnv/stg/dm_customer_engagement_snapshot_hdp";
end if

print("is_dev - $is_dev");
print("path_dir - $path_dir");

TRY
    delete_hdfs_dir($path_dir);
CATCH ex THEN
    print("Trunc source User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

if ($is_dev == 'y') then
    DROP TABLE IF EXISTS custom_rb_vnv_stg.dm_customer_engagement_snapshot_hdp;
    CREATE EXTERNAL TABLE custom_rb_vnv_stg.dm_customer_engagement_snapshot_hdp(
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
    snapshot_dt string)
    STORED AS PARQUET
    LOCATION
      '/data/custom/rb/vnv/stg/dm_customer_engagement_snapshot_hdp';
else
    DROP TABLE IF EXISTS custom_rb_greenplum_stg.dm_customer_engagement_snapshot_hdp;
    CREATE EXTERNAL TABLE custom_rb_greenplum_stg.dm_customer_engagement_snapshot_hdp(
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
    snapshot_dt string)
    STORED AS PARQUET
    LOCATION
      '/data/custom/rb/greenplum/stg/dm_customer_engagement_snapshot_hdp';
end if
