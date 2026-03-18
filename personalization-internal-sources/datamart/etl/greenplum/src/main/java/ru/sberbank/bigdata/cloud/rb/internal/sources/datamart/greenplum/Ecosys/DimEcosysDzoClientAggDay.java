package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.Ecosys;

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
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_18_4;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rb_depfin.dim_ecosys_dzo_client_agg_day", name = "Dim ecosys dzo client agg day", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "d2", saveRemover = UpdatedPartitionRemover.class)

public class DimEcosysDzoClientAggDay extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(DimEcosysDzoClientAggDay.class);
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("d2").create());
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
                .appName("greenplum_stg_dim_ecosys_dzo_client_agg_day")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "dim_ecosys_dzo_client_agg_hdp")
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
                ? col("d2").lt(buildDate())
                : col("d2").geq(startDt).and(col("d2").lt(endDt))
                .and(col("period_id").equalTo("1"));

        Dataset<Row> result = sourceTable
                .where(filter)
                .select(
                        col("bns_lty_cnt").cast(LongType).as("bns_lty_cnt"),
                        col("evt_cnt").cast(LongType).as("evt_cnt"),
                        col("partner_id").cast(LongType).as("partner_id"),
                        col("epk_id").cast(LongType).as("epk_id"),
                        col("evt_id").cast(IntegerType).as("evt_id"),
                        col("period_id").cast(IntegerType).as("period_id"),
                        col("src_flg").cast(StringType).as("src_flg"),
                        col("host_user_id").cast(StringType).as("host_user_id"),
                        col("d2").cast(StringType).as("d2"),
                        col("d1").cast(StringType).as("d1"),
                        col("row_update_dtime").cast(TimestampType).as("row_update_dtime"),
                        col("bns_lty_amt").cast(DECIMAL_18_4).as("bns_lty_amt"),
                        col("evt_qty").cast(DECIMAL_18_4).as("evt_qty"),
                        col("evt_amt").cast(DECIMAL_18_4).as("evt_amt")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(DimEcosysDzoClientAggDay.class);
    }
}