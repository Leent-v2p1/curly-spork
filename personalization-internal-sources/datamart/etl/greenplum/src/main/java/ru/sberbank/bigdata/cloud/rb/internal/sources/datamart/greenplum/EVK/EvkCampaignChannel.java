package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVK;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_evk.ref_union_campaign_channel", name = "Справочник каналов для ЕВК", useSystemPropertyToGetId = true)
@FullReplace
public class EvkCampaignChannel extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvkCampaignChannel.class);
    private String gpurl;
    private String user;

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evk_campaign_channel")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> refUnionCampaignChannel = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "ref_union_campaign_channel")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", "channel_id")
                .load();

        return refUnionCampaignChannel;
    }

    public static void main(String[] args) {
        runner().run(EvkCampaignChannel.class);
    }
}
