let path_dir = "/data/custom/rb/greenplum/stg/ref_union_campaign_dic_hdp";
let is_dev = '${$is_dev}';
let need_trunc_source = '${$need_trunc_source}';

if ($is_dev == 'y') then
    let path_dir = "/data/custom/rb/evk/stg/ref_union_campaign_dic_hdp";
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
        DROP TABLE IF EXISTS custom_rb_evk_stg.ref_union_campaign_dic_hdp;
        CREATE EXTERNAL TABLE custom_rb_evk_stg.ref_union_campaign_dic_hdp(
           `sk_id` bigint,
          `ab_test_id` bigint,
          `product_id` string,
          `product_name` string,
          `product_group_name` string,
          `product_sgroup_name` string,
          `start_dt` string,
          `end_dt` string,
          `wave_launch_dt` string,
          `wave_deactivation_dt` string,
          `unique_segment` string,
          `segment` string,
          `campaign_code` string,
          `campaign_name` string,
          `campaign_name_cf` string,
          `campaign_type` string,
          `rnk` int,
          `campaign_id` string,
          `segment_id` string,
          `wave_id` string,
          `lal_id` string,
          `camp_source` string,
          `message_id` string,
          `template_text` string,
          `channel_id` int,
          `channel_name` string,
          `product_group_id` string,
          `tg` string,
          `template_comment` string,
          `template_title` string,
          `channel_name_crm` string,
          `offer_id` string,
          `team_id` string,
          `channel_type` string,
          `channel_name_rep` string,
          `channel_system` string,
          `camp_source_process` string,
          `auto_launch_flg` int,
          `campaign_trgt_type_name` string,
          `template_text_full` string,
          `template_name` string,
          `params_txt` string,
          `team_name` string,
          `is_manual` int,
          `load_ts` timestamp)
        STORED AS PARQUET
        LOCATION
            '/data/custom/rb/evk/stg/ref_union_campaign_dic_hdp';
    else
        DROP TABLE IF EXISTS custom_rb_greenplum_stg.ref_union_campaign_dic_hdp;
        CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ref_union_campaign_dic_hdp(
          `sk_id` bigint,
          `ab_test_id` bigint,
          `product_id` string,
          `product_name` string,
          `product_group_name` string,
          `product_sgroup_name` string,
          `start_dt` string,
          `end_dt` string,
          `wave_launch_dt` string,
          `wave_deactivation_dt` string,
          `unique_segment` string,
          `segment` string,
          `campaign_code` string,
          `campaign_name` string,
          `campaign_name_cf` string,
          `campaign_type` string,
          `rnk` int,
          `campaign_id` string,
          `segment_id` string,
          `wave_id` string,
          `lal_id` string,
          `camp_source` string,
          `message_id` string,
          `template_text` string,
          `channel_id` int,
          `channel_name` string,
          `product_group_id` string,
          `tg` string,
          `template_comment` string,
          `template_title` string,
          `channel_name_crm` string,
          `offer_id` string,
          `team_id` string,
          `channel_type` string,
          `channel_name_rep` string,
          `channel_system` string,
          `camp_source_process` string,
          `auto_launch_flg` int,
          `campaign_trgt_type_name` string,
          `template_text_full` string,
          `template_name` string,
          `params_txt` string,
          `team_name` string,
          `is_manual` int,
          `load_ts` timestamp)
        STORED AS PARQUET
        LOCATION
          '/data/custom/rb/greenplum/stg/ref_union_campaign_dic_hdp';
    end if

    else
        print("No need trunc source");
end if