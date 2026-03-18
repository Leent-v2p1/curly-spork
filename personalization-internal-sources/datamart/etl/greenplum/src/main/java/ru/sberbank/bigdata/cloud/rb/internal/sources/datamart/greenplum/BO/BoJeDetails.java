package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.BO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_bo.v_is0121_bo_je_details_h", name = "Bo Je details", useSystemPropertyToGetId = true)
@FullReplace
public class BoJeDetails extends Datamart {
    private static final Logger log = LoggerFactory.getLogger(BoJeDetails.class);
    private String gpurl;
    private String user;
    private String partitionColumn;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("report_dt").create());
    }




    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_bo_je_details")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");
        partitionColumn = spark.conf().get("spark.jdbc.partitionColumn");

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> boJeDetails = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_bf_vd")
                .option("dbtable", "v_is0121_bo_je_details_h")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> result = boJeDetails
                .select(
                        col("agrmnt_id"),
                        col("amount_lcl"),
                        col("bankcommission"),
                        col("classcode"),
                        col("contragent"),
                        col("coupon1"),
                        col("coupon2"),
                        col("currency"),
                        col("date2"),
                        col("epk_id"),
                        col("external_id"),
                        col("isrepo").cast(IntegerType).as("isrepo"),
                        col("market_type").cast(IntegerType).as("market_type"),
                        col("operation_id"),
                        col("operation_time"),
                        col("operationcomment"),
                        col("period_type"),
                        col("price1"),
                        col("price2"),
                        col("rate"),
                        col("report_dt").cast(StringType).as("report_dt"),
                        col("security_id").cast(IntegerType).as("security_id"),
                        col("security_id_new"),
                        col("term"),
                        col("trans_cnt"),
                        col("trans_type").cast(IntegerType).as("trans_type"),
                        col("tscommission"),
                        col("uid"),
                        col("volumeoperation2")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(BoJeDetails.class);
    }
}
