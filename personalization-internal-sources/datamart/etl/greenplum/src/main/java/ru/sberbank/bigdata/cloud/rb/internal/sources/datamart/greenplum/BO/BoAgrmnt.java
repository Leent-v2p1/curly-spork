package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.BO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.FullReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_bo.v_bo_agrmnt", name = "V Bo Agreement", useSystemPropertyToGetId = true)
@FullReplace
public class BoAgrmnt extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(BoAgrmnt.class);
    private String gpurl;
    private String user;
    private String partitionColumn;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("agrmnt_open_dt").create());
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_v_bo_agrmnt")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");
        partitionColumn = spark.conf().get("spark.jdbc.partitionColumn");

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> vBoAgrmnt = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_bf_vd")
                .option("dbtable", "v_bo_agrmnt")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        Dataset<Row> result = vBoAgrmnt
                .select(
                        col("bo_agrmnt_id"),
                        col("agrmnt_code"),
                        col("row_actual_from_dt").cast(StringType).as("row_actual_from_dt"),
                        col("row_actual_to_dt").cast(StringType).as("row_actual_to_dt"),
                        col("row_actual_flag"),
                        col("agrmnt_open_dt").cast(StringType).as("agrmnt_open_dt"),
                        col("agrmnt_close_dt").cast(StringType).as("agrmnt_close_dt"),
                        col("epk_id"),
                        col("tariff_id").cast(IntegerType).as("tariff_id"),
                        col("qistatus"),
                        col("iis_flag"),
                        col("ins_upd")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(BoAgrmnt.class);
    }
}
