package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EVK;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.SixMonthsRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.greenplum.EvkTables;

import java.text.SimpleDateFormat;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;


@PartialReplace(partitioning = "date_build_snp", saveRemover = SixMonthsRemover.class)
@DatamartRef(id = "custom_rb_evk.dm_union_campaign_history_snp", name = "Snapshot dm_union_campaign_history")
public class EvkCampaignHistSnp extends Datamart {

    private static final String DATE_BUILD_SNP = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis());
    private LocalDate startDt;
    private LocalDate endDt;
    private final Integer DEFAULT_DEPTH_MONTHLY = 6;

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        final ParametersService parametersService = datamartServiceFactory.parametersService();
        Integer depthMonthly = SysPropertyTool.getIntSystemProperty("spark.calculate_depth_monthly").orElse(DEFAULT_DEPTH_MONTHLY);
        this.startDt = parametersService.startCtlParameter().orElse(buildDate()
                .minusMonths(isFirstLoading ? depthMonthly : 1).withDayOfMonth(1));
        this.endDt = parametersService.endCtlParameter().orElse(buildDate());
    }

    @Override
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        final PartitionInfo triggersPartitioning = PartitionInfo.dynamic().add("date_build_snp").add("start_dt").create();
        return new PartitionedSavingStrategy(triggersPartitioning);
    }

    @Override
    public Dataset<Row> buildDatamart() {
        final Dataset<Row> evk = sourceTable(EvkTables.DM_UNION_CAMPAIGN_HISTORY);

        return evk.where(
                        col("start_dt").geq(startDt)
                            .and(col("start_dt").lt(endDt)))
                .select(
                        col("sk_id"),
                        col("epk_id"),
                        col("channel_id").cast(IntegerType),
                        col("start_dt").cast(StringType),
                        col("end_dt").cast(StringType),
                        col("member_id"),
                        col("member_code"),
                        col("cg_flg").cast(IntegerType),
                        col("contact_ts"),
                        col("delivery_ts"),
                        col("open_ts"),
                        col("started_ts"),
                        col("initial_ts"),
                        col("sub_channel"),
                        col("load_ts"),
                        col("cg_stat_flg").cast(IntegerType),
                        col("params_txt")
                )
                .withColumn("date_build_snp", lit(DATE_BUILD_SNP).cast(StringType));
    }

    public static void main(String[] args) {
        runner().run(EvkCampaignHistSnp.class);
    }
}
