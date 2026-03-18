let path_dirs =
["dm_union_campaign_history_hdp",
"dm_union_campaign_history_hdp_1",
"dm_union_campaign_history_hdp_2"];

let is_dev = '${$is_dev}';
let need_trunc_source = '${$need_trunc_source}';

if ($is_dev == 'y') then
    let path_dirs =
    ["dm_union_campaign_history_hdp",
    "dm_union_campaign_history_hdp_1",
    "dm_union_campaign_history_hdp_2"];
end if

print("is_dev - $is_dev");
print("path_dir - $path_dirs");

if($need_trunc_source == 'y') then

    if ($is_dev == 'y') then

        for table_name in $path_dirs loop

            TRY
                delete_hdfs_dir("/data/custom/rb/evk/stg/$table_name");
                CATCH ex THEN
                    print("Trunc source User exception message:");
                    let res = $ex["type"];
                    print($res);
                    let res_msg = $ex["message"];
                    print($res_msg);
            END

            DROP TABLE IF EXISTS custom_rb_evk_stg.$table_name;
            CREATE EXTERNAL TABLE custom_rb_evk_stg.$table_name(
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
              `start_dt` string)
            STORED AS PARQUET
            LOCATION
                '/data/custom/rb/evk/stg/$table_name';

        end loop;
    else

        for table_name in $path_dirs loop

             TRY
                 delete_hdfs_dir("/data/custom/rb/greenplum/stg/$table_name");
                 CATCH ex THEN
                     print("Trunc source User exception message:");
                     let res = $ex["type"];
                     print($res);
                     let res_msg = $ex["message"];
                     print($res_msg);
             END

             DROP TABLE IF EXISTS custom_rb_greenplum_stg.$table_name;
             CREATE EXTERNAL TABLE custom_rb_greenplum_stg.$table_name(
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
               `start_dt` string)
             STORED AS PARQUET
             LOCATION
                 '/data/custom/rb/greenplum/stg/$table_name';

         end loop;
    end if
    else
        print("No need to trunc source");
end if