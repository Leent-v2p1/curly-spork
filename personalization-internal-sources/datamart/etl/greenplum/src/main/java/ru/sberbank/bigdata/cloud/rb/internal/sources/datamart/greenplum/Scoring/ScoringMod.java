package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.Scoring;

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

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_18_4;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_scoring.t_epk_scoring_platform_mod_new2", name = "Витрина ГП по данным рисков", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "row_actual_to", saveRemover = UpdatedPartitionRemover.class)
public class ScoringMod extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(ScoringMod.class);
    private String gpurl;
    private String user;
    private String row_loading_id;
    private String row_update_dt;
    private Date endDt;
    private Date startDt;


    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("report_dt_part").add("product_type_id", IntegerType).create());
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        this.startDt = Date.valueOf(parametersService.startCtlParameter()
                .orElse(buildDate()));
        this.endDt = Date.valueOf(parametersService.endCtlParameter()
                .orElse(buildDate().plusMonths(1).withDayOfMonth(1)));
    }

    @Override
    public Dataset<Row> buildDatamart() {

        log.warn("startDt: {}, endDt: {}", startDt, endDt);

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_scoring_mod")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");
        row_loading_id = spark.conf().get("spark.ctl.loading.id");
        row_update_dt = spark.conf().get("spark.start.time");

        Dataset<Row> tEpkScoringPlatformMod = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_hdp")
                .option("dbtable", "t_epk_scoring_platform_mod")
                .option("url", gpurl)
                .option("user", user)
                .option("driver", "org.postgresql.Driver")
                .option("pool.maxSize", "5")
                .option("server.nic", "eth1")
                .load();

        addStatistic(PROCESSED_LOADING_ID, buildDate().toString());
        disableDefaultStatistics();

        final Dataset<Row> gpFiltered = tEpkScoringPlatformMod
                .filter(col("report_dt").geq(startDt)
                        .and(col("report_dt").lt(endDt)));

        Dataset<Row> result = gpFiltered
                .select(
                        col("report_dt").cast(StringType),
                        col("offer_from_dt").cast(StringType),
                        col("offer_to_dt").cast(StringType),
                        col("product_type_id").cast(IntegerType),
                        col("offer_type_id").cast(IntegerType),
                        col("gov_supp_flag").cast(IntegerType),
                        col("init_pymnt_pct_backet_code").cast(StringType),
                        col("annuity_pymnt_rub_amt").cast(DECIMAL_18_4),
                        col("cust_test_grp_send_masspers_code").cast(StringType),
                        col("epk_id"),
                        col("segment_business_cd"),
                        col("segment_tech_cd"),
                        col("rate"),
                        col("currency_cd"),
                        col("limit_6m_amt"),
                        col("limit_12m_amt"),
                        col("limit_18m_amt"),
                        col("limit_24m_amt"),
                        col("limit_36m_amt"),
                        col("limit_48m_amt"),
                        col("limit_60m_amt"),
                        col("limit_max_amt"),
                        col("tb_cd"),
                        col("score_zone"),
                        col("init_contrib_pct"),
                        col("term_max"),
                        col("subprod_id_ret"),
                        col("subpr_id_expr"),
                        col("result_id").cast(IntegerType),
                        col("top_up_amount"),
                        col("pd"),
                        col("calcv"),
                        col("src_id_offer"),
                        col("approval_rate"),
                        col("banner_flag").cast(IntegerType),
                        col("limit_marketing"),
                        regexp_replace(col("report_dt"), "-", "").alias("report_dt_part"))
                .withColumn("row_loading_id", lit(row_loading_id))
                .withColumn("row_update_dt", lit(row_update_dt))
                .withColumn("row_scr_loading_id", lit("-1"));


        return result;
    }

    public static void main(String[] args) {
        runner().run(ScoringMod.class);
    }
}
