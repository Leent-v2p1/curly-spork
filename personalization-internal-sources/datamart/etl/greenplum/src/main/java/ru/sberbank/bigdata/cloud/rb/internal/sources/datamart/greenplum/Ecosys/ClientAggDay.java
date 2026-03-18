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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.PROCESSED_LOADING_ID;

@DatamartRef(id = "custom_rb_depfin.dim_ecosys_client_agg_day", name = "Dim Ecosys Client Agg Day", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = "report_dt", saveRemover = UpdatedPartitionRemover.class)

public class ClientAggDay extends Datamart {
    private static final Logger log = LoggerFactory.getLogger(ClientAggDay.class);
    private String gpurl;
    private String user;
    private String partitionColumn;
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
        this.startDt = Date.valueOf(parametersService.startCtlParameter()
                .orElse(buildDate().minusMonths(1).withDayOfMonth(1)));
    }

    @Override
    public Dataset<Row> buildDatamart() {

        SparkSession spark = SparkSession
                .builder()
                .appName("greenplum_stg_dim_ecosys_client_agg_day")
                .enableHiveSupport()
                .getOrCreate();

        gpurl = spark.conf().get("spark.jdbc.gpurl");
        user = spark.conf().get("spark.jdbc.gpUser");
        partitionColumn = spark.conf().get("spark.jdbc.partitionColumn");

        log.info("gpUrl: {}, gpuser: {}, partitionColumn: {}", gpurl, user, partitionColumn);

        Dataset<Row> dimEcosysClientAgg = spark.read().format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
                .option("dbschema", "s_grnplm_vd_rozn_mpp_aaas_vd")
                .option("dbtable", "dim_ecosys_client_agg_day")
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
                : col("report_dt").gt(startDt);

        return dimEcosysClientAgg
                .where(filter)
                .select(
                        col("client_gosb"),
                        col("txn_dt"),
                        col("update_ts"),
                        col("partner_id"),
                        col("partner_type"),
                        col("txn_cnt_serv"),
                        col("txn_cnt_ecom_serv"),
                        col("client_city"),
                        col("client_city_2"),
                        col("partner_name"),
                        col("report_dt").cast(StringType).as("report_dt"),
                        col("client_city_size"),
                        col("client_region"),
                        col("txn_sum_serv"),
                        col("txn_sum_ecom_serv"),
                        col("client_tb"),
                        col("sber_eco_flag"),
                        col("epk_id"),
                        col("partner_type_id"));
    }

    public static void main(String[] args) {
        runner().run(ClientAggDay.class);
    }
}
