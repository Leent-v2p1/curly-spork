let path_dir = "/data/custom/rb/greenplum/stg/ref_union_campaign_channel";
let is_dev = '${$is_dev}';
let need_trunc_source = '${$need_trunc_source}';

if ($is_dev == 'y') then
    let path_dir = "/data/custom/rb/evk/stg/ref_union_campaign_channel";
end if

print("is_dev - $is_dev");
print("path_dir - $path_dir");

if($need_trunc_source == 'y') then
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
        DROP TABLE IF EXISTS custom_rb_evk_stg.ref_union_campaign_channel;
        CREATE EXTERNAL TABLE custom_rb_evk_stg.ref_union_campaign_channel(
            channel_id	bigint,
            channel_name	string,
            channel_type	string,
            channel_name_desc	string,
            channel_system	string,
            ctl_loading	int,
            ctl_validfrom	timestamp,
            ctl_action	string)
        STORED AS PARQUET
        LOCATION
            '/data/custom/rb/evk/stg/ref_union_campaign_channel';
    else
        DROP TABLE IF EXISTS custom_rb_greenplum_stg.ref_union_campaign_channel;
        CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ref_union_campaign_channel(
            channel_id	bigint,
            channel_name	string,
            channel_type	string,
            channel_name_desc	string,
            channel_system	string,
            ctl_loading	int,
            ctl_validfrom	timestamp,
            ctl_action	string)
        STORED AS PARQUET
        LOCATION
          '/data/custom/rb/greenplum/stg/ref_union_campaign_channel';
    end if
    else
    print("No need trunc source");

end if