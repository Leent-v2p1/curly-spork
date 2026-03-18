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

DROP TABLE IF EXISTS custom_rb_greenplum_stg.ref_union_campaign_channel;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ref_union_campaign_channel(
    `channel_id` bigint, 
	`channel_name` string, 
	`channel_type` string, 
	`channel_name_desc` string, 
	`channel_system` string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/ref_union_campaign_channel';


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

DROP TABLE IF EXISTS custom_rb_greenplum_stg.dm_union_campaign_history_hdp;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.dm_union_campaign_history_hdp(
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
	`start_dt` string
)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/dm_union_campaign_history_hdp';
  
DROP TABLE IF EXISTS custom_rb_greenplum_stg.dm_union_campaign_history_hdp_1;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.dm_union_campaign_history_hdp_1(
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
	`start_dt` string
)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/dm_union_campaign_history_hdp_1';
  
DROP TABLE IF EXISTS custom_rb_greenplum_stg.dm_union_campaign_history_hdp_2;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.dm_union_campaign_history_hdp_2(
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
	`start_dt` string
)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/dm_union_campaign_history_hdp_2';


CREATE EXTERNAL TABLE if not exists custom_rb_evk.dm_union_campaign_history(
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
	  `ctl_loading` int, 
	  `ctl_validfrom` timestamp, 
	  `ctl_action` string)
	PARTITIONED BY ( 
	  `start_dt` string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/evk/pa/dm_union_campaign_history';
  
CREATE EXTERNAL TABLE if not exists `custom_rb_evk.dm_union_campaign_history_snp`(
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
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
PARTITIONED BY ( 
  `date_build_snp` string, 
  `start_dt` string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/evk/pa/dm_union_campaign_history_snp';
  
CREATE EXTERNAL TABLE  if not exists  `custom_rb_evk.ft_nba_pkd_flag_flat_act`(
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
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
PARTITIONED BY ( 
  `report_dt` string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/evk/pa/ft_nba_pkd_flag_flat_act';

CREATE EXTERNAL TABLE   if not exists  `custom_rb_evk.ref_union_campaign_channel`(
  `channel_id` bigint, 
  `channel_name` string, 
  `channel_type` string, 
  `channel_name_desc` string, 
  `channel_system` string, 
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/evk/pa/ref_union_campaign_channel';

CREATE EXTERNAL TABLE   if not exists  `custom_rb_evk.ref_union_campaign_channel_snp`(
  `channel_id` bigint, 
  `channel_name` string, 
  `channel_type` string, 
  `channel_name_desc` string, 
  `channel_system` string, 
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
PARTITIONED BY ( 
  `date_build_snp` string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/evk/pa/ref_union_campaign_channel_snp';

CREATE EXTERNAL TABLE   if not exists  `custom_rb_evk.ref_union_campaign_dic_hdp`(
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
  `load_ts` timestamp, 
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS PARQUET
LOCATION
  '/data/custom/rb/evk/pa/ref_union_campaign_dic_hdp';

CREATE EXTERNAL TABLE    if not exists   `custom_rb_evk.ref_union_campaign_dic_hdp_snp`(
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
  `load_ts` timestamp, 
  `ctl_loading` int, 
  `ctl_validfrom` timestamp, 
  `ctl_action` string)
PARTITIONED BY ( 
  `date_build_snp` string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/evk/pa/ref_union_campaign_dic_hdp_snp';




