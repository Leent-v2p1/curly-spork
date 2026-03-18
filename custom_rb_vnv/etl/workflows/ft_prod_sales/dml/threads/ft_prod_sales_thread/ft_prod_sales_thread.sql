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
    let startDt = (SELECT trunc(add_months(CURRENT_DATE, -1), 'MM'))[0][0];
end if

if $endDt == "" then
    let endDt = (SELECT CURRENT_DATE)[0][0];
end if

print("startDt - $startDt");
print("endDt - $endDt");

-- Очистка stg-области перед расчетом.
TRY
delete_hdfs_dir("/data/custom/rb/vnv/stg/ft_prod_sales_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

DROP TABLE IF EXISTS custom_rb_vnv_stg.ft_prod_sales_stg;
CREATE EXTERNAL TABLE custom_rb_vnv_stg.ft_prod_sales_stg(
    sale_sk string,
    sale_type_id int,
    sale_product_class_id int,
    sale_product_group1_name string,
    sale_product_group2_name string,
    sale_product_group3_name string,
    host_agrmnt_id bigint,
    orig_agrmnt_exp_dt string,
    epk_id bigint,
    insert_ts timestamp,
    npv_avg decimal(38,26),
    extra_json string,
    product_class_name string,
    sale_type_name string,
    sale_channel_name string,
    ctl_loading int,
    ctl_validfrom timestamp,
    ctl_action string
)
PARTITIONED BY (sale_dt string)
STORED AS PARQUET
LOCATION
'/data/custom/rb/vnv/stg/ft_prod_sales_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');
MSCK REPAIR TABLE custom_rb_vnv_stg.ft_prod_sales_stg;

-- Формируем инкремент.

if ($typeWf == "arc") then
    INSERT OVERWRITE TABLE custom_rb_vnv_stg.ft_prod_sales_stg PARTITION(sale_dt)
    SELECT
      cast(sale_sk as string) as sale_sk,
      cast(sale_type_id as int) as sale_type_id,
      cast(sale_product_class_id as int) as sale_product_class_id,
      cast(sale_product_group1_name as string) as sale_product_group1_name,
      cast(sale_product_group2_name as string) as sale_product_group2_name,
      cast(sale_product_group3_name as string) as sale_product_group3_name,
      cast(host_agrmnt_id as bigint) as host_agrmnt_id,
      cast(orig_agrmnt_exp_dt as string) as orig_agrmnt_exp_dt,
      cast(epk_id as bigint) as epk_id,
      cast(insert_ts as timestamp) as insert_ts,
      cast(npv_avg as decimal(38,26)) as npv_avg,
      cast(extra_json as string) as extra_json,
      cast(product_class_name as string) as product_class_name,
      cast(sale_type_name as string) as sale_type_name,
      cast(sale_channel_name as string) as sale_channel_name,
      $ctlLoading as ctl_loading,
      current_timestamp() as ctl_validfrom,
      "I" as ctl_action,
      cast(sale_dt as string) as sale_dt
    from ${$src_schema_name}.ft_prod_sales_hdp;
end if

if ($typeWf == "inc") then
    INSERT OVERWRITE TABLE custom_rb_vnv_stg.ft_prod_sales_stg PARTITION(sale_dt)
    SELECT
      cast(sale_sk as string) as sale_sk,
      cast(sale_type_id as int) as sale_type_id,
      cast(sale_product_class_id as int) as sale_product_class_id,
      cast(sale_product_group1_name as string) as sale_product_group1_name,
      cast(sale_product_group2_name as string) as sale_product_group2_name,
      cast(sale_product_group3_name as string) as sale_product_group3_name,
      cast(host_agrmnt_id as bigint) as host_agrmnt_id,
      cast(orig_agrmnt_exp_dt as string) as orig_agrmnt_exp_dt,
      cast(epk_id as bigint) as epk_id,
      cast(insert_ts as timestamp) as insert_ts,
      cast(npv_avg as decimal(38,26)) as npv_avg,
      cast(extra_json as string) as extra_json,
      cast(product_class_name as string) as product_class_name,
      cast(sale_type_name as string) as sale_type_name,
      cast(sale_channel_name as string) as sale_channel_name,
      $ctlLoading as ctl_loading,
      current_timestamp() as ctl_validfrom,
      "I" as ctl_action,
      cast(sale_dt as string) as sale_dt
    from ${$src_schema_name}.ft_prod_sales_hdp
    where sale_dt >= to_date('$startDt','yyyy-MM-dd') and sale_dt <= to_date('$endDt','yyyy-MM-dd');
end if

coalesce_files(
	"/data/custom/rb/vnv/stg/ft_prod_sales_stg",
	"/data/custom/rb/vnv/stg/ft_prod_sales_temp",
	128, "snappy", CAST($parallelDegree as INT)
);

--backup_partitioned_auto("ft_prod_sales", "bkp");

move_table_to_schema(
    s2tTableList="ft_prod_sales_stg->ft_prod_sales",
    srcSchema="custom_rb_vnv_stg",
    tgtSchema="custom_rb_vnv",
    compressionType="snappy",
    workMode=$typeWf,
    truncateIncFilterList="",
    truncateArcFilterList="ft_prod_sales->all",
    instanceFilter="",
    truncateStgFromPa=true
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);