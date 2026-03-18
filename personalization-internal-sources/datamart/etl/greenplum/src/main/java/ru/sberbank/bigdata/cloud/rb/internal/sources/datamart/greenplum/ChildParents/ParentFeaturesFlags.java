package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.ChildParents;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_20_10;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import java.sql.Date;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_20_4;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.GreenplumConstants.*;

@DatamartRef(id = "custom_rozn_family.ft_parent_features_flags", name = "Parent features flag", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "report_dt", saveRemover = UpdatedPartitionRemover.class)
public class ParentFeaturesFlags extends Datamart {
    private static final Logger log = LoggerFactory.getLogger(ParentFeaturesFlags.class);
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("report_dt").create());
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.endDt = Date.valueOf(parametersService.endCtlParameter().orElse(buildDate()));
        this.startDt = Date.valueOf(parametersService.startCtlParameter().orElse(buildDate().minusMonths(1).withDayOfMonth(1)));
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_family.ft_parent_features_flags")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = Optional.of(spark.conf().get("spark.jdbc.gpurl")).orElse(GP_URL);
        String user = Optional.of(spark.conf().get("spark.jdbc.gpUser")).orElse(GP_USER);
        String partitionColumn = Optional.of(spark.conf().get("spark.jdbc.partitionColumn")).orElse(GP_PARTITION_COLUMN);

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> parentFeaturesFlags = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_hdp_vd")
                .option("dbtable", "ft_parent_features_flags")
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
                : col("report_dt").gt(startDt).and(col("report_dt").leq(endDt));

        Dataset<Row> sourceTable = parentFeaturesFlags
                .where(filter)
                .select(
                        col("has_child_3yr_5yr_flg").cast(BooleanType).as("has_child_3yr_5yr_flg"),
                        col("has_child_3yr_7yr_flg").cast(BooleanType).as("has_child_3yr_7yr_flg"),
                        col("has_child_5yr_6yr_flg").cast(BooleanType).as("has_child_5yr_6yr_flg"),
                        col("has_child_15yr_18yr_flg").cast(BooleanType).as("has_child_15yr_18yr_flg"),
                        col("has_child_almost_6yr_flg").cast(BooleanType).as("has_child_almost_6yr_flg"),
                        col("has_child_almost_1yr_flg").cast(BooleanType).as("has_child_almost_1yr_flg"),
                        col("has_child_3yr_6yr_flg").cast(BooleanType).as("has_child_3yr_6yr_flg"),
                        col("has_child_1_5yr_3yr_flg").cast(BooleanType).as("has_child_1_5yr_3yr_flg"),
                        col("bmm_flg").cast(BooleanType).as("bmm_flg"),
                        col("has_child_7yr_10yr_flg").cast(BooleanType).as("has_child_7yr_10yr_flg"),
                        col("has_child_6yr_14yr_flg").cast(BooleanType).as("has_child_6yr_14yr_flg"),
                        col("has_child_7yr_14yr_flg").cast(BooleanType).as("has_child_7yr_14yr_flg"),
                        col("has_child_13yr_15yr_flg").cast(BooleanType).as("has_child_13yr_15yr_flg"),
                        col("has_child_almost_2yr_flg").cast(BooleanType).as("has_child_almost_2yr_flg"),
                        col("has_child_0yr_3yr_flg").cast(BooleanType).as("has_child_0yr_3yr_flg"),
                        col("has_child_0yr_7yr_flg").cast(BooleanType).as("has_child_0yr_7yr_flg"),
                        col("has_child_0yr_18yr_flg").cast(BooleanType).as("has_child_0yr_18yr_flg"),
                        col("has_child_almost_5yr_flg").cast(BooleanType).as("has_child_almost_5yr_flg"),
                        col("has_child_1yr_3yr_flg").cast(BooleanType).as("has_child_1yr_3yr_flg"),
                        col("has_child_almost_18yr_flg").cast(BooleanType).as("has_child_almost_18yr_flg"),
                        col("aliment_receiver_flg").cast(BooleanType).as("aliment_receiver_flg"),
                        col("has_child_0yr_1_5yr_flg").cast(BooleanType).as("has_child_0yr_1_5yr_flg"),
                        col("has_child_10yr_13yr_flg").cast(BooleanType).as("has_child_10yr_13yr_flg"),
                        col("has_child_14yr_18yr_flg").cast(BooleanType).as("has_child_14yr_18yr_flg"),
                        col("has_child_almost_3yr_flg").cast(BooleanType).as("has_child_almost_3yr_flg"),
                        col("has_child_7yr_13yr_flg").cast(BooleanType).as("has_child_7yr_13yr_flg"),
                        col("has_child_almost_14yr_flg").cast(BooleanType).as("has_child_almost_14yr_flg"),
                        col("has_child_almost_4yr_flg").cast(BooleanType).as("has_child_almost_4yr_flg"),
                        col("has_child_0yr_1yr_flg").cast(BooleanType).as("has_child_0yr_1yr_flg"),
                        col("has_child_5yr_7yr_flg").cast(BooleanType).as("has_child_5yr_7yr_flg"),
                        col("epk_id").cast(LongType).as("epk_id"),
                        col("pl_school_ap_num").cast(IntegerType).as("pl_school_ap_num"),
                        col("school_ap_num").cast(IntegerType).as("school_ap_num"),
                        col("birth_dict_num").cast(IntegerType).as("birth_dict_num"),
                        col("child_num").cast(IntegerType).as("child_num"),
                        col("aliment_model_num").cast(IntegerType).as("aliment_model_num"),
                        col("years3_model_num").cast(IntegerType).as("years3_model_num"),
                        col("maternity_model_num").cast(IntegerType).as("maternity_model_num"),
                        col("benefits_model_num").cast(IntegerType).as("benefits_model_num"),
                        col("covid_0_8_child_num").cast(IntegerType).as("covid_0_8_child_num"),
                        col("orphans_model_num").cast(IntegerType).as("orphans_model_num"),
                        col("benefits_type_num").cast(IntegerType).as("benefits_type_num"),
                        col("months18_model_num").cast(IntegerType).as("months18_model_num"),
                        col("covid_0_3_child_num").cast(IntegerType).as("covid_0_3_child_num"),
                        col("child_many_type_num").cast(IntegerType).as("child_many_type_num"),
                        col("years3_dict_num").cast(IntegerType).as("years3_dict_num"),
                        col("pregnancy_model_num").cast(IntegerType).as("pregnancy_model_num"),
                        col("disabled_model_num").cast(IntegerType).as("disabled_model_num"),
                        col("pregnancy_dict_num").cast(IntegerType).as("pregnancy_dict_num"),
                        col("covid_6_18_child_num").cast(IntegerType).as("covid_6_18_child_num"),
                        col("covid_3_16_child_num").cast(IntegerType).as("covid_3_16_child_num"),
                        col("child_brand_txn_num").cast(IntegerType).as("child_brand_txn_num"),
                        col("source_cd").cast(StringType).as("source_cd"),
                        col("report_dt").cast(StringType).as("report_dt"),
                        col("covid_0_8_last_payment_dt").cast(DateType).as("covid_0_8_last_payment_dt"),
                        col("covid_3_16_last_payment_dt").cast(DateType).as("covid_3_16_last_payment_dt"),
                        col("birth_predict_dt").cast(DateType).as("birth_predict_dt"),
                        col("covid_0_3_last_payment_dt").cast(DateType).as("covid_0_3_last_payment_dt"),
                        col("covid_6_18_last_payment_dt").cast(DateType).as("covid_6_18_last_payment_dt"),
                        col("child_brand_txn_amt").cast(DECIMAL_20_4).as("child_brand_txn_amt"),
                        col("covid_0_3_amt").cast(DECIMAL_20_4).as("covid_0_3_amt"),
                        col("weight_prc").cast(DECIMAL_20_10).as("weight_prc"),
                        col("covid_0_8_amt").cast(DECIMAL_20_4).as("covid_0_8_amt"),
                        col("pl_school_ap_amt").cast(DECIMAL_20_4).as("pl_school_ap_amt"),
                        col("school_ap_amt").cast(DECIMAL_20_4).as("school_ap_amt"),
                        col("covid_3_16_amt").cast(DECIMAL_20_4).as("covid_3_16_amt"),
                        col("maternity_type_amt").cast(DECIMAL_20_4).as("maternity_type_amt"),
                        col("covid_6_18_amt").cast(DECIMAL_20_4).as("covid_6_18_amt")
                );

        return sourceTable;
    }
    public static void main(String[] args) {
        runner().run(ParentFeaturesFlags.class);
    }
}

