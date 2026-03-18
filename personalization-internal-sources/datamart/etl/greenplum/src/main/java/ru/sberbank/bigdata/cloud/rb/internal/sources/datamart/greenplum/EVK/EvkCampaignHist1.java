package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVK;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import java.sql.Date;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_evk.dm_union_campaign_history_hdp_1", name = "Справочник кампаний для ЕВК", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "start_dt", saveRemover = UpdatedPartitionRemover.class)
public class EvkCampaignHist1 extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvkCampaignHist1.class);
    private String gpurl;
    private String user;
    private String partCol;
    private String partNum;

    public static final Object[] CHANNEL_ID = {-3007, -2005, 4, 1008, 1118, 1193};  // договора Комерцбанка и Кредит-Москва

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("start_dt").create());
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evk_campaign_hist_1")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");
        partCol = spark.conf().get("spark.jdbc.partCol");
        log.info("gpUrl: {}, gpuser: {}", gpurl, user);

        Dataset<Row> refUnionCampaignHist1 = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_core")
                .option("dbtable", "dm_union_campaign_history_hdp_1")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partCol)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> result = refUnionCampaignHist1
                .select(
                        col("sk_id"),
                        col("epk_id"),
                        col("channel_id").cast(IntegerType),
                        col("start_dt").cast(StringType),
                        col("end_dt").cast(StringType),
                        col("member_id"),
                        col("member_code"),
                        col("cg_flg").cast(IntegerType),
                        col("contact_ts"),
                        col("delivery_ts"),
                        col("open_ts"),
                        col("started_ts"),
                        col("initial_ts"),
                        col("sub_channel"),
                        col("load_ts"),
                        col("cg_stat_flg").cast(IntegerType),
                        col("params_txt")
                ).repartition(10);

        return result;
    }

    public static void main(String[] args) {
        runner().run(EvkCampaignHist1.class);
    }
}