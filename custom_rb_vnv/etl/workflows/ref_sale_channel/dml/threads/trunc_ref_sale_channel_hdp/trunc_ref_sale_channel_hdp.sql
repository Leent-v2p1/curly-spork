let path_dir = "/data/custom/rb/greenplum/stg/ref_sale_channel_hdp";
let is_dev = '${$is_dev}';

if ($is_dev == 'y') then
    let path_dir = "/data/custom/rb/vnv/stg/ref_sale_channel_hdp";
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
    DROP TABLE IF EXISTS custom_rb_vnv_stg.ref_sale_channel_hdp;
    CREATE EXTERNAL TABLE custom_rb_vnv_stg.ref_sale_channel_hdp(
    sale_channel_cd int,
    sale_channel_id int,
    sale_channel_name string,
    sale_channel_source string)
    STORED AS PARQUET
    LOCATION
      '/data/custom/rb/vnv/stg/ref_sale_channel_hdp';
else
    DROP TABLE IF EXISTS custom_rb_greenplum_stg.ref_sale_channel_hdp;
    CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ref_sale_channel_hdp(
    sale_channel_cd int,
    sale_channel_id int,
    sale_channel_name string,
    sale_channel_source string)
    STORED AS PARQUET
    LOCATION
      '/data/custom/rb/greenplum/stg/ref_sale_channel_hdp';
end if