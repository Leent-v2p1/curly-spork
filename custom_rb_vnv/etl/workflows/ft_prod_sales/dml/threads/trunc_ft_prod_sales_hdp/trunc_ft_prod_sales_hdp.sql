let path_dir = "/data/custom/rb/greenplum/stg/ft_prod_sales_hdp";
let is_dev = '${$is_dev}';

if ($is_dev == 'y') then
    let path_dir = "/data/custom/rb/vnv/stg/ft_prod_sales_hdp";
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
    DROP TABLE IF EXISTS custom_rb_vnv_stg.ft_prod_sales_hdp;
    CREATE EXTERNAL TABLE custom_rb_vnv_stg.ft_prod_sales_hdp(
    sale_sk string,
    sale_type_id int,
    sale_product_class_id int,
    sale_product_group1_name string,
    sale_product_group2_name string,
    sale_product_group3_name string,
    host_agrmnt_id numeric(38,0),
    orig_agrmnt_exp_dt string,
    epk_id bigint,
    insert_ts timestamp,
    npv_avg decimal(38,26),
    extra_json string,
    product_class_name string,
    sale_type_name string,
    sale_channel_name string,
    sale_dt string)
    STORED AS PARQUET
    LOCATION
      '/data/custom/rb/vnv/stg/ft_prod_sales_hdp';
else
    DROP TABLE IF EXISTS custom_rb_greenplum_stg.ft_prod_sales_hdp;
    CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ft_prod_sales_hdp(
    sale_sk string,
    sale_type_id int,
    sale_product_class_id int,
    sale_product_group1_name string,
    sale_product_group2_name string,
    sale_product_group3_name string,
    host_agrmnt_id numeric(38,0),
    orig_agrmnt_exp_dt string,
    epk_id bigint,
    insert_ts timestamp,
    npv_avg decimal(38,26),
    extra_json string,
    product_class_name string,
    sale_type_name string,
    sale_channel_name string,
    sale_dt string)
    STORED AS PARQUET
    LOCATION
      '/data/custom/rb/greenplum/stg/ft_prod_sales_hdp';
end if