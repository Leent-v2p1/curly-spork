package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVO;

import org.apache.spark.sql.Column;
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
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_0;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_26;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.v_da_evo_kpi", name = "Ref v_da_evo_kpi", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "report_dt", saveRemover = UpdatedPartitionRemover.class)

public class EvoKpi extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoKpi.class);
    private Date endDt;
    private Date startDt;

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
                .appName("greenplum_stg_v_da_evo_kpi")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "v_da_evo_kpi_hdp")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        final Column filter = lit("1").equalTo(lit("1"));

        Dataset<Row> result = sourceTable
                .where(filter)
                .select(
                        col("dict_team_id").cast(IntegerType).as("dict_team_id"),
                        col("datetime_insert").cast(TimestampType).as("datetime_insert"),
                        col("dict_activity_type_id").cast(IntegerType).as("dict_activity_type_id"),
                        col("dict_comm_channel_id").cast(IntegerType).as("dict_comm_channel_id"),
                        col("dict_metric_id").cast(IntegerType).as("dict_metric_id"),
                        col("dict_product_and_product_group_id").cast(IntegerType).as("dict_product_and_product_group_id"),
                        col("dict_sale_channel_id").cast(IntegerType).as("dict_sale_channel_id"),
                        col("dict_sale_type_id").cast(IntegerType).as("dict_sale_type_id"),
                        col("dict_seg_rb_id").cast(IntegerType).as("dict_seg_rb_id"),
                        col("fact_cnt").cast(DECIMAL_38_26).as("fact_cnt"),
                        col("is_mnl_fact").cast(IntegerType).as("is_mnl_fact"),
                        col("is_salt_product").cast(IntegerType).as("is_salt_product"),
                        col("plan_cnt").cast(DECIMAL_38_26).as("plan_cnt"),
                        col("report_dt").cast(StringType).as("report_dt"),
                        col("team").cast(StringType).as("team"),
                        col("product").cast(StringType).as("product"),
                        col("product_group").cast(StringType).as("product_group"),
                        col("sale_type").cast(StringType).as("sale_type"),
                        col("comm_channel").cast(StringType).as("comm_channel"),
                        col("sale_channel").cast(StringType).as("sale_channel"),
                        col("seg_rb").cast(StringType).as("seg_rb"),
                        col("activity_type").cast(StringType).as("activity_type"),
                        col("metric").cast(StringType).as("metric"),
                        col("forecast").cast(DECIMAL_38_26).as("forecast")
                );
        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoKpi.class);
    }
}