package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.Scoring;

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
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.GP_URL;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.GP_USER;

@DatamartRef(id = "custom_rb_scoring.t_epk_f_top_up", name = "Epk Top Up", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "report_date", saveRemover = UpdatedPartitionRemover.class)
public class EpkTopUp extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(EpkTopUp.class);
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("report_date").create());
    }

    @Override
    public void addDatafixes() {
        dataFixApi.castColumnIfNotCasted("credit_amount_initial", DECIMAL_15_2);
        dataFixApi.castColumnIfNotCasted("basic_rate", DECIMAL_12_2);
        dataFixApi.castColumnIfNotCasted("full_early_repayment_amount", DECIMAL_19_2);
        dataFixApi.castColumnIfNotCasted("annuity_payment", DECIMAL_12_2);
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.endDt = Date.valueOf(parametersService.endCtlParameter().orElse(buildDate()));
        this.startDt = Date.valueOf(parametersService.startCtlParameter()
                .orElse(buildDate().minusMonths(2).withDayOfMonth(1)));
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_t_epk_f_top_up")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse("epk_id");

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> topUp = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_hdp")
                .option("dbtable", "t_epk_f_top_up")
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
                : col("report_date").gt(startDt).and(col("report_date").leq(endDt));

        Dataset<Row> result = topUp
                .where(filter)
                .select(
                        col("report_date").cast(StringType).as("report_date"),
                        col("epk_id"),
                        col("agreement_count"),
                        col("credit_type"),
                        col("credit_agreement"),
                        col("date_of_credit_issue"),
                        col("bank"),
                        col("credit_amount_initial").cast(DECIMAL_15_2).as("credit_amount_initial"),
                        col("currency"),
                        col("crediting_period_months"),
                        col("basic_rate").cast(DECIMAL_12_2).as("basic_rate"),
                        col("annuity_payment").cast(DECIMAL_12_2).as("annuity_payment"),
                        col("plan_repayment_date"),
                        col("full_early_repayment_amount").cast(DECIMAL_19_2).as("full_early_repayment_amount"),
                        col("baks"),
                        col("contract_src_code")
                );

        return result;
    }

    public static void main(String[] args) {
        runner().run(EpkTopUp.class);
    }
}
