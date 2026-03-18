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

DROP TABLE IF EXISTS custom_rb_greenplum_stg.ref_sale_channel_hdp;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ref_sale_channel_hdp(
sale_channel_cd int,
sale_channel_id int,
sale_channel_name string,
sale_channel_source string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/ref_sale_channel_hdp';

DROP TABLE IF EXISTS custom_rb_greenplum_stg.ref_product_class_hdp;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ref_product_class_hdp(
dwh_info_system_type_cd string,
product_class_id int,
product_class_name string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/ref_product_class_hdp';

DROP TABLE IF EXISTS custom_rb_greenplum_stg.ref_sale_type_hdp;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.ref_sale_type_hdp(
sale_type_id int,
sale_type_name string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/ref_sale_type_hdp';

DROP TABLE IF EXISTS custom_rb_greenplum_stg.dm_customer_engagement_snapshot_hdp;
CREATE EXTERNAL TABLE custom_rb_greenplum_stg.dm_customer_engagement_snapshot_hdp(
customer_id bigint,
customer_status string,
segment_code string,
segment_name string,
region_name string,
city_name string,
preferred_channel string,
last_contact_dt string,
last_purchase_dt string,
active_product_cnt int,
avg_monthly_turnover decimal(18,2),
loyalty_score int,
churn_probability decimal(5,4),
campaign_eligible_flg int,
snapshot_dt string)
STORED AS PARQUET
LOCATION
  '/data/custom/rb/greenplum/stg/dm_customer_engagement_snapshot_hdp';

