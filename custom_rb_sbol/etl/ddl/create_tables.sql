
CREATE DATABASE IF NOT EXISTS custom_rb_sbol_stg;
CREATE DATABASE IF NOT EXISTS custom_rb_sbol;

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr (
  epk_id bigint,
  fst_logon_dt timestamp,
  lst_logon_dt timestamp,
  tot_qty bigint,
  fst_mobil_dt timestamp,
  lst_mobil_dt timestamp,
  mobil_qty bigint,
  fst_web_dt timestamp,
  lst_web_dt timestamp,
  web_qty bigint,
  fst_atm_dt timestamp,
  lst_atm_dt timestamp,
  atm_qty bigint,
  fst_arm_dt timestamp,
  lst_arm_dt timestamp,
  arm_qty bigint,
  fst_sberid_dt timestamp,
  lst_sberid_dt timestamp,
  sberid_qty bigint,
  tot_day_qty bigint,
  mobil_day_qty bigint,
  web_day_qty bigint,
  atm_day_qty bigint,
  arm_day_qty bigint,
  sberid_day_qty bigint,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (month_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_for_mega (
  epk_id bigint,
  sbol_fst_login_ever_dt string,
  sbol_atm_fst_login_ever_dt string,
  sbol_mob_fst_login_ever_dt string,
  sbol_web_fst_login_ever_dt string,
  sbol_lst_login_ever_dt string,
  sbol_atm_lst_login_ever_dt string,
  sbol_mob_lst_login_ever_dt string,
  sbol_web_lst_login_ever_dt string,
  sbol_fst_login_dt timestamp,
  sbol_atm_fst_login_dt timestamp,
  sbol_mob_fst_login_dt timestamp,
  sbol_web_fst_login_dt timestamp,
  sbol_lst_login_dt timestamp,
  sbol_atm_lst_login_dt timestamp,
  sbol_mob_lst_login_dt timestamp,
  sbol_web_lst_login_dt timestamp,
  sbol_1m_login_flag int,
  sbol_atm_1m_login_flag int,
  sbol_mob_1m_login_flag int,
  sbol_web_1m_login_flag int,
  sbol_3m_login_flag int,
  sbol_atm_3m_login_flag int,
  sbol_mob_3m_login_flag int,
  sbol_web_3m_login_flag int,
  sbol_mnth_fst_dt_qty double,
  sbol_mnth_lst_dt_qty double,
  sbol_mnth_atm_lst_dt_qty double,
  sbol_mnth_mob_lst_dt_qty double,
  sbol_mnth_web_lst_dt_qty double,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (month_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_for_mega';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_hist (
  epk_id bigint,
  fst_logon_ever_dt timestamp,
  lst_logon_ever_dt timestamp,
  fst_mobil_ever_dt timestamp,
  lst_mobil_ever_dt timestamp,
  fst_web_ever_dt timestamp,
  lst_web_ever_dt timestamp,
  fst_atm_ever_dt timestamp,
  lst_atm_ever_dt timestamp,
  fst_arm_ever_dt timestamp,
  lst_arm_ever_dt timestamp,
  fst_sberid_ever_dt timestamp,
  lst_sberid_ever_dt timestamp,
  login_id decimal(38,0),
  device_info string,
  application string,
  channel_type string,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_hist';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_oper_aggr (
  epk_id bigint,
  login_type string,
  exec_rub_total decimal(38,15),
  exec_rub_web decimal(38,15),
  exec_rub_mobil decimal(38,15),
  exec_rub_atm decimal(38,15),
  exec_rub_mb decimal(38,15),
  exec_rub_other decimal(38,15),
  exec_qty bigint,
  exec_web_qty bigint,
  exec_mobil_qty bigint,
  exec_atm_qty bigint,
  exec_mb_qty bigint,
  exec_other_qty bigint,
  fst_exec_dt string,
  lst_exec_dt string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (month_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_oper_aggr';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_oper_aggr_for_mega (
  epk_id bigint,
  sbol_fst_txn_dt string,
  sbol_lst_txn_dt string,
  sbol_fst_txn_ever_dt string,
  sbol_lst_txn_ever_dt string,
  sbol_txn_1m_amt decimal(38,15),
  sbol_txn_1m_qty bigint,
  sbol_txn_3m_amt decimal(38,15),
  sbol_txn_3m_qty bigint,
  sbol_txn_atm_1m_amt decimal(38,15),
  sbol_txn_atm_1m_qty bigint,
  sbol_txn_atm_3m_amt decimal(38,15),
  sbol_txn_atm_3m_qty bigint,
  sbol_txn_mob_1m_amt decimal(38,15),
  sbol_txn_mob_1m_qty bigint,
  sbol_txn_mob_3m_amt decimal(38,15),
  sbol_txn_mob_3m_qty bigint,
  sbol_txn_web_1m_amt decimal(38,15),
  sbol_txn_web_1m_qty bigint,
  sbol_txn_web_3m_amt decimal(38,15),
  sbol_txn_web_3m_qty bigint,
  sbol_txn_main_1m_amt decimal(38,15),
  sbol_txn_main_1m_qty bigint,
  sbol_txn_main_3m_amt decimal(38,15),
  sbol_txn_main_3m_qty bigint,
  sbol_txn_mb_1m_amt decimal(38,15),
  sbol_txn_mb_1m_qty bigint,
  sbol_txn_mb_3m_amt decimal(38,15),
  sbol_txn_mb_3m_qty bigint,
  sbol_txn_other_1m_amt decimal(38,15),
  sbol_txn_other_1m_qty bigint,
  sbol_txn_other_3m_amt decimal(38,15),
  sbol_txn_other_3m_qty bigint,
  sbol_mnth_fst_dt_qty double,
  sbol_mnth_lst_dt_qty double,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (month_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_oper_aggr_for_mega';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl2 (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl2';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl3 (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl3';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl4 (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl4';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl5 (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl5';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl_gf (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl_gf';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl6 (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl6';

CREATE EXTERNAL TABLE IF NOT EXISTS custom_rb_sbol.sbol_logon_aggr_dt_ikfl7 (
  login_id decimal(38,0),
  application string,
  channel_type string,
  logon_dt timestamp,
  epk_id bigint,
  device_info string,
  min_logon_dt timestamp,
  max_logon_dt timestamp,
  cnt_logon bigint,
  tb_nmb string,
  osb_nmb string,
  vsp_nmb string,
  schema string,
  ctl_loading int,
  ctl_validfrom timestamp,
  ctl_action string
)
PARTITIONED BY (day_part string)
STORED AS PARQUET
LOCATION '/data/custom/rb/sbol/pa/sbol_logon_aggr_dt_ikfl7';
