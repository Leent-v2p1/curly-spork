package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.Ecosys;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.LongType;
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
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rb_depfin.ft_ecosys_txn_det", name = "Ft ecosys txn det", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "txn_dt", saveRemover = UpdatedPartitionRemover.class)

public class FtEcosysTxnDet extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(FtEcosysTxnDet.class);
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("txn_dt").create());
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.endDt = Date.valueOf(parametersService.endCtlParameter().orElse(buildDate()));
        this.startDt = Date.valueOf(parametersService.startCtlParameter()
                .orElse(buildDate().minusMonths(1).withDayOfMonth(1)));
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_ft_ecosys_txn_det")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "ft_ecosys_txn_det_hdp")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .option("partitionColumn", partitionColumn)
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        final Column filter = isFirstLoading
                ? col("txn_dt").lt(buildDate())
                : col("txn_dt").geq(startDt).and(col("txn_dt").lt(endDt));

        Dataset<Row> result = sourceTable
                .where(filter)
                .select(
                        col("txn_sum_cr").cast(LongType).as("txn_sum_cr"),
                        col("txn_cnt_cr").cast(LongType).as("txn_cnt_cr"),
                        col("txn_sum_db").cast(LongType).as("txn_sum_db"),
                        col("txn_cnt_db").cast(LongType).as("txn_cnt_db"),
                        col("sber_eco_flag").cast(LongType).as("sber_eco_flag"),
                        col("merchid").cast(LongType).as("merchid"),
                        col("partner_id").cast(LongType).as("partner_id"),
                        col("partner_type_id").cast(LongType).as("partner_type_id"),
                        col("epk_id").cast(LongType).as("epk_id"),
                        col("ecom_fl").cast(IntegerType).as("ecom_fl"),
                        col("mcc_code").cast(StringType).as("mcc_code"),
                        col("txn_dt").cast(StringType).as("txn_dt"),
                        col("row_update_dtime").cast(TimestampType).as("row_update_dtime")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(FtEcosysTxnDet.class);
    }
}