let typeWf = '${$type}';
print("type - $typeWf");
let parallelDegree = '${$parallel_degree}';
print("parallelDegree - $parallelDegree");
let ctlLoading = '${$loading_id}';
print("ctlLoading - $ctlLoading");
let businessDate = (SELECT date_format(date_add(current_timestamp(), -1), 'yyyy-MM-dd'))[0][0];
print("businessDate - $businessDate");

-- Очистка stg-области перед расчетом.
TRY
delete_hdfs_dir("/data/custom/rb/vnv/stg/ref_product_class_stg");
CATCH ex THEN
    print("User exception message:");
    let res = $ex["type"];
    print($res);
    let res_msg = $ex["message"];
    print($res_msg);
END

DROP TABLE IF EXISTS custom_rb_vnv_stg.ref_product_class_stg;
CREATE EXTERNAL TABLE custom_rb_vnv_stg.ref_product_class_stg(
    dwh_info_system_type_cd string,
    product_class_id int,
    product_class_name string,
    ctl_loading int,
    ctl_validfrom timestamp,
    ctl_action string
)
STORED AS PARQUET
LOCATION
'/data/custom/rb/vnv/stg/ref_product_class_stg'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY', 'transactional'='false');


INSERT OVERWRITE TABLE custom_rb_vnv_stg.ref_product_class_stg
SELECT
	cast(dwh_info_system_type_cd as string) as dwh_info_system_type_cd,
	cast(product_class_id as int) as product_class_id,
	cast(product_class_name as string) as product_class_name,
    $ctlLoading as ctl_loading,
    current_timestamp() as ctl_validfrom,
    "I" as ctl_action
FROM ${$src_schema_name}.ref_product_class_hdp;

coalesce_files(
	"/data/custom/rb/vnv/stg/ref_product_class_stg",
	"/data/custom/rb/vnv/stg/ref_product_class_temp",
	128, "snappy", CAST($parallelDegree as INT)
);

--backup_partitioned_auto("ref_product_class", "bkp");

move_table_to_schema(
    s2tTableList="ref_product_class_stg->ref_product_class",
    srcSchema="custom_rb_vnv_stg",
    tgtSchema="custom_rb_vnv",
    compressionType="snappy",
    workMode=$typeWf,
    truncateIncFilterList="",
    truncateArcFilterList="ref_product_class->all",
    instanceFilter="",
    truncateStgFromPa=false
);

let lastLoadedTime = (select date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm'Z'"))[0][0];
print("lastLoadedTime - $lastLoadedTime");

let meta = '{2 : "true", 5 : "$businessDate", 10: "$ctlLoading", 11: "$lastLoadedTime", 34 : "$typeWf"}';
print("meta - $meta");
publish($meta);