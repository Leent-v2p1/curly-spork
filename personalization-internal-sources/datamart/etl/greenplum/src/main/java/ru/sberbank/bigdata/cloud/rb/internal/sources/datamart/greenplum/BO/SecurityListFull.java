package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.BO;

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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_8;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_bo.v_ref_is0121_dic_security_list_full", name = "Ref Dic Security List full", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "execdate", saveRemover = UpdatedPartitionRemover.class)
public class SecurityListFull extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(SecurityListFull.class);
    private Date endDt;
    private Date startDt;

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return new PartitionedSavingStrategy(PartitionInfo.dynamic().add("execdate").create());
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
                .appName("greenplum_stg_ref_is0121_dic_security_list_full")
                .enableHiveSupport()
                .getOrCreate();

        String gpurl = spark.conf().get("spark.jdbc.gpurl");
        String user = spark.conf().get("spark.jdbc.gpUser");
        String partitionColumn = spark.conf().get("spark.jdbc.partitionColumn");

        log.info("gpurl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> securityListFull = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_daas_bf_vd")
                .option("dbtable", "v_ref_is0121_dic_security_list_full")
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
                : col("execdate").gt(startDt).and(col("execdate").leq(endDt));

        return securityListFull
                .where(filter)
                .select(
                        col("briefname").cast(StringType).as("briefname"),
                        col("dateend").cast(DateType).as("dateend"),
                        col("datestart").cast(DateType).as("datestart"),
                        col("emitent").cast(StringType).as("emitent"),
                        col("endservicedate").cast(DateType).as("endservicedate"),
                        col("execdate").cast(StringType).as("execdate"),
                        col("fullname").cast(StringType).as("fullname"),
                        col("isin").cast(StringType).as("isin"),
                        col("issueplace").cast(StringType).as("issueplace"),
                        col("nominal").cast(DECIMAL_38_8).as("nominal"),
                        col("nominalfund").cast(StringType).as("nominalfund"),
                        col("primedispositiondate").cast(DateType).as("primedispositiondate"),
                        col("regnum").cast(StringType).as("regnum"),
                        col("securityid").cast(LongType).as("securityid"),
                        col("ticker").cast(StringType).as("ticker"),
                        col("typecb").cast(StringType).as("typecb"),
                        col("vidcb").cast(StringType).as("vidcb")
                );
    }

    public static void main(String[] args) {
        runner().run(SecurityListFull.class);
    }
}
