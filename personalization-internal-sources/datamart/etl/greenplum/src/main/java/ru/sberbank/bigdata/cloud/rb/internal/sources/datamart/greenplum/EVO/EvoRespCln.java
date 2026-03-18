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
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_18_4;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_evo.da_evo_resp_cln_hdp", name = "Ref da_evo_resp_cln_hdp", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "first_response_dt", saveRemover = UpdatedPartitionRemover.class)
public class EvoRespCln extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EvoRespCln.class);
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("first_response_dt").create());
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.endDt = Date.valueOf(parametersService.endCtlParameter().orElse(buildDate()));
        this.startDt = Date.valueOf(parametersService.startCtlParameter().orElse(buildDate().minusDays(210).withDayOfMonth(1)));
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_evo_resp_cln_hdp")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> sourceTable = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "da_evo_resp_cln_hdp")
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
                ? lit("1").equalTo(lit("1"))
                : col("first_response_dt").geq(startDt).and(col("first_response_dt").leq(endDt));

        Dataset<Row> result = sourceTable
                .where(filter)
                .select(
                        col("first_response_dt").cast(StringType).as("first_response_dt"),
                        col("last_response_dt").cast(DateType).as("last_response_dt"),
                        col("camp_start_dt").cast(DateType).as("camp_start_dt"),
                        col("rule_sk_id").cast(IntegerType).as("rule_sk_id"),
                        col("cg_flg").cast(IntegerType).as("cg_flg"),
                        col("resp_cnt").cast(IntegerType).as("resp_cnt"),
                        col("unique_only_nflag").cast(IntegerType).as("unique_only_nflag"),
                        col("ab_test_id").cast(LongType).as("ab_test_id"),
                        col("epk_id").cast(LongType).as("epk_id"),
                        col("client_dk").cast(LongType).as("client_dk"),
                        col("target_vl").cast(DECIMAL_18_4).as("target_vl"),
                        col("npv").cast(DECIMAL_18_4).as("npv"),
                        col("first_deal_params").cast(StringType).as("first_deal_params"),
                        col("changed_ts").cast(TimestampType).as("changed_ts"),
                        col("product_group").cast(StringType).as("product_group"),
                        col("changed_user").cast(StringType).as("changed_user")
                );
        return result;
    }

    public static void main(String[] args) {
        runner().run(EvoRespCln.class);
    }
}