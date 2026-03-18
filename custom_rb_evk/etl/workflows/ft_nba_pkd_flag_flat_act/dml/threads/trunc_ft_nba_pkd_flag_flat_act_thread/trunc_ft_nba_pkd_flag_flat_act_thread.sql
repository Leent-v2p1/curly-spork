let path_dir = "/data/custom/rb/greenplum/stg/ft_nba_pkd_flag_flat_act";
let is_dev = '${$is_dev}';
let need_trunc_source = '${$need_trunc_source}';

if ($is_dev == 'y') then
    let path_dir = "/data/custom/rb/evk/stg/ft_nba_pkd_flag_flat_act";
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
        DROP TABLE IF EXISTS custom_rb_evk_stg.ft_nba_pkd_flag_flat_act;
        CREATE EXTERNAL TABLE custom_rb_evk_stg.ft_nba_pkd_flag_flat_act(
          `epk_id` bigint,
          `cmpn_sms_avail_nflag_no_sl` int,
          `cmpn_sms_avail_nflag` int,
          `cmpn_push_sbol_avail_nflag_no_sl` int,
          `cmpn_push_sbol_avail_nflag` int,
          `cmpn_push_sms_avail_nflag_no_sl` int,
          `cmpn_push_sms_avail_nflag` int,
          `cmpn_tm_avail_nflag_no_sl` int,
          `cmpn_tm_avail_nflag` int,
          `cmpn_tm_robot_avail_nflag_no_sl` int,
          `cmpn_tm_robot_avail_nflag` int,
          `cmpn_push_sberinvestor_avail_nflag_no_sl` int,
          `cmpn_push_sberinvestor_avail_nflag` int,
          `cmpn_email_avail_nflag_no_sl` int,
          `cmpn_email_avail_nflag` int,
          `cmpn_channel_erkc_ivr_in_avail_nflag_no_sl` int,
          `cmpn_channel_erkc_ivr_in_avail_nflag` int,
          `cmpn_channel_sitesber_avail_nflag_no_sl` int,
          `cmpn_channel_sitesber_avail_nflag` int,
          `cmpn_channel_sbolpro_avail_nflag_no_sl` int,
          `cmpn_channel_sbolpro_avail_nflag` int,
          `cmpn_channel_kmpremierpull_avail_nflag_no_sl` int,
          `cmpn_channel_kmpremierpull_avail_nflag` int,
          `cmpn_banner_mpsbol_avail_nflag_no_sl` int,
          `cmpn_banner_mpsbol_avail_nflag` int,
          `cmpn_banner_sberinvestor_avail_nflag_no_sl` int,
          `cmpn_banner_sberinvestor_avail_nflag` int,
          `cmpn_banner_websbol_avail_nflag_no_sl` int,
          `cmpn_banner_websbol_avail_nflag` int,
          `cmpn_dsa_avail_nflag_no_sl` int,
          `cmpn_dsa_avail_nflag` int,
          `cmpn_dkm_avail_nflag_no_sl` int,
          `cmpn_dkm_avail_nflag` int,
          `cmpn_sberads_avail_nflag_no_sl` int,
          `cmpn_sberads_avail_nflag` int,
          `cmpn_va_avail_nflag_no_sl` int,
          `cmpn_va_avail_nflag` int,
          `cmpn_push_km_sb1_avail_nflag_no_sl` int,
          `cmpn_push_km_sb1_avail_nflag` int,
          `cmpn_channel_kmsb1pull_avail_nflag_no_sl` int,
          `cmpn_channel_kmsb1pull_avail_nflag` int,
          `cmpn_atm_avail_nflag` int,
          `cmpn_atm_avail_nflag_no_sl` int,
          `cmpn_km_b2b_avail_nflag` int,
          `cmpn_km_b2b_avail_nflag_no_sl` int,
          `cmpn_push_km_pb_avail_nflag` int,
          `cmpn_push_km_pb_avail_nflag_no_sl` int,
          `cmpn_push_km_premier_avail_nflag` int,
          `cmpn_push_km_premier_avail_nflag_no_sl` int,
          `cmpn_sb_bol_avail_nflag` int,
          `cmpn_sb_bol_avail_nflag_no_sl` int,
          `cmpn_sber_id_avail_nflag` int,
          `cmpn_sber_id_avail_nflag_no_sl` int,
          `report_dt` string)
        STORED AS PARQUET
        LOCATION
            '/data/custom/rb/evk/stg/ft_nba_pkd_flag_flat_act';
    else
        DROP TABLE IF EXISTS custom_rb_greenplum_stg.ft_nba_pkd_flag_flat_act;
        CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ft_nba_pkd_flag_flat_act(
          `epk_id` bigint,
          `cmpn_sms_avail_nflag_no_sl` int,
          `cmpn_sms_avail_nflag` int,
          `cmpn_push_sbol_avail_nflag_no_sl` int,
          `cmpn_push_sbol_avail_nflag` int,
          `cmpn_push_sms_avail_nflag_no_sl` int,
          `cmpn_push_sms_avail_nflag` int,
          `cmpn_tm_avail_nflag_no_sl` int,
          `cmpn_tm_avail_nflag` int,
          `cmpn_tm_robot_avail_nflag_no_sl` int,
          `cmpn_tm_robot_avail_nflag` int,
          `cmpn_push_sberinvestor_avail_nflag_no_sl` int,
          `cmpn_push_sberinvestor_avail_nflag` int,
          `cmpn_email_avail_nflag_no_sl` int,
          `cmpn_email_avail_nflag` int,
          `cmpn_channel_erkc_ivr_in_avail_nflag_no_sl` int,
          `cmpn_channel_erkc_ivr_in_avail_nflag` int,
          `cmpn_channel_sitesber_avail_nflag_no_sl` int,
          `cmpn_channel_sitesber_avail_nflag` int,
          `cmpn_channel_sbolpro_avail_nflag_no_sl` int,
          `cmpn_channel_sbolpro_avail_nflag` int,
          `cmpn_channel_kmpremierpull_avail_nflag_no_sl` int,
          `cmpn_channel_kmpremierpull_avail_nflag` int,
          `cmpn_banner_mpsbol_avail_nflag_no_sl` int,
          `cmpn_banner_mpsbol_avail_nflag` int,
          `cmpn_banner_sberinvestor_avail_nflag_no_sl` int,
          `cmpn_banner_sberinvestor_avail_nflag` int,
          `cmpn_banner_websbol_avail_nflag_no_sl` int,
          `cmpn_banner_websbol_avail_nflag` int,
          `cmpn_dsa_avail_nflag_no_sl` int,
          `cmpn_dsa_avail_nflag` int,
          `cmpn_dkm_avail_nflag_no_sl` int,
          `cmpn_dkm_avail_nflag` int,
          `cmpn_sberads_avail_nflag_no_sl` int,
          `cmpn_sberads_avail_nflag` int,
          `cmpn_va_avail_nflag_no_sl` int,
          `cmpn_va_avail_nflag` int,
          `cmpn_push_km_sb1_avail_nflag_no_sl` int,
          `cmpn_push_km_sb1_avail_nflag` int,
          `cmpn_channel_kmsb1pull_avail_nflag_no_sl` int,
          `cmpn_channel_kmsb1pull_avail_nflag` int,
          `cmpn_atm_avail_nflag` int,
          `cmpn_atm_avail_nflag_no_sl` int,
          `cmpn_km_b2b_avail_nflag` int,
          `cmpn_km_b2b_avail_nflag_no_sl` int,
          `cmpn_push_km_pb_avail_nflag` int,
          `cmpn_push_km_pb_avail_nflag_no_sl` int,
          `cmpn_push_km_premier_avail_nflag` int,
          `cmpn_push_km_premier_avail_nflag_no_sl` int,
          `cmpn_sb_bol_avail_nflag` int,
          `cmpn_sb_bol_avail_nflag_no_sl` int,
          `cmpn_sber_id_avail_nflag` int,
          `cmpn_sber_id_avail_nflag_no_sl` int,
          `report_dt` string)
        STORED AS PARQUET
        LOCATION
          '/data/custom/rb/greenplum/stg/ft_nba_pkd_flag_flat_act';
    end if

    else
        print("No need to trunc source");
end if