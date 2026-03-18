package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVK;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_evk.ref_union_campaign_dic_hdp", name = "Справочник кампаний для ЕВК", useSystemPropertyToGetId = true)
@FullReplace
public class EvkCampaignDic extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvkCampaignDic.class);
    private String gpurl;
    private String user;

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evk_campaign_dic")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");

        log.info("gpUrl: {}, gpuser: {}", gpurl, user);

        Dataset<Row> refUnionCampaignDic = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "ref_union_campaign_dic_hdp")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", "sk_id")
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> result = refUnionCampaignDic
                .select(
                        col("sk_id"),
                        col("ab_test_id"),
                        col("product_id"),
                        col("product_name"),
                        col("product_group_name"),
                        col("product_sgroup_name"),
                        col("start_dt").cast(StringType),
                        col("end_dt").cast(StringType),
                        col("wave_launch_dt").cast(StringType),
                        col("wave_deactivation_dt").cast(StringType),
                        col("unique_segment"),
                        col("segment"),
                        col("campaign_code"),
                        col("campaign_name"),
                        col("campaign_name_cf"),
                        col("campaign_type"),
                        col("rnk"),
                        col("campaign_id"),
                        col("segment_id"),
                        col("wave_id"),
                        col("lal_id"),
                        col("camp_source"),
                        col("message_id"),
                        col("template_text"),
                        col("channel_id").cast(IntegerType),
                        col("channel_name"),
                        col("product_group_id"),
                        col("tg"),
                        col("template_comment"),
                        col("template_title"),
                        col("channel_name_crm"),
                        col("offer_id"),
                        col("team_id"),
                        col("channel_type"),
                        col("channel_name_rep"),
                        col("channel_system"),
                        col("camp_source_process"),
                        col("auto_launch_flg").cast(IntegerType),
                        col("campaign_trgt_type_name"),
                        col("template_text_full"),
                        col("template_name"),
                        col("params_txt"),
                        col("team_name"),
                        col("is_manual"),
                        col("load_ts")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(EvkCampaignDic.class);
    }
}
