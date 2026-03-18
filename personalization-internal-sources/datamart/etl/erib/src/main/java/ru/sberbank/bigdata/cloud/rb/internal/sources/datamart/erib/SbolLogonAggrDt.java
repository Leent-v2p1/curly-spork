package ru.sberbank.bigdata.cloud.rb.internal.sources.datamart.erib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.DatamartRef;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.PartialReplace;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.ParametersService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.DbNameImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover.UpdatedPartitionRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.DataFrameUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.AutoConfigDatamartRunner.runner;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaNames.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.DecimalTypes.DECIMAL_38_0;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.DAY_PART;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.DAY_PART_FORMAT;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.LAST_LOAD_START;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.startOfDay;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.isEmpty;

@DatamartRef(id = "sbol_logon_aggr_dt", name = "Ежедневные агрегаты по входам в СБОЛ", useSystemPropertyToGetId = true)
@PartialReplace(partitioning = DAY_PART, saveRemover = UpdatedPartitionRemover.class)
public class SbolLogonAggrDt extends Datamart {

    private static final Logger log = LoggerFactory.getLogger(SbolLogonAggrDt.class);
    public static final StructType SBOL_LOGON_AGGR_DT_SCHEMA = new StructType()
            .add("login_id", DECIMAL_38_0)
            .add("application", StringType)
            .add("channel_type", StringType)
            .add("logon_dt", TimestampType)
            .add("epk_id", LongType)
            .add("device_info", StringType)
            .add("min_logon_dt", TimestampType)
            .add("max_logon_dt", TimestampType)
            .add("cnt_logon", LongType)
            .add("tb_nmb", StringType)
            .add("osb_nmb", StringType)
            .add("vsp_nmb", StringType)
            .add("schema", StringType)
            .add("day_part", StringType);

    public static final String TYPE = "LOGON";
    public static final Object[] CHANNELS = {"MAPI", "WEB", "SBOL"};

    private Date lastAggregationDate;
    private String tableForSchema;
    private String sourceSchema;
    private String newSourceShema;

    private String migrationDate;
    private String newSource;

    private Timestamp startTime;
    private Timestamp endTime;
    private String startDt;
    private String endDt;
    private boolean reload;


    private static final LocalDate DEFAULT_START_DT = LocalDate.of(2015, Month.JANUARY, 1);

    @Override
    public void addDatafixes() {
        dataFixApi.castColumnIfNotCasted("epk_id", LongType);
        dataFixApi.addColumnIfMissing("channel_type", StringType);
    }

    @Override
    public void init(DatamartServiceFactory datamartServiceFactory) {
        migrationDate = SysPropertyTool.getSystemProperty("spark.migrationDate", "2024-08-01"); //дата миграции на источник launcher
        newSource= SysPropertyTool.getSystemProperty("spark.newSource", "1");
        newSourceShema= SysPropertyTool.getSystemProperty("spark.newSourceShema", "custom_rb_sbol");

        final ParametersService ps = datamartServiceFactory.parametersService();
        this.reload = ps.forceReload();
        if (isFirstLoading || reload) {
            LocalDateTime startDt = ps.startCtlParameter()
                    .orElse(DEFAULT_START_DT)
                    .atStartOfDay();
            LocalDateTime endDt = ps.endCtlParameter().
                    orElse(buildDate())
                    .atStartOfDay();
            this.endTime = Timestamp.valueOf(endDt);
            this.startTime = Timestamp.valueOf(startDt);
            this.endDt = String.valueOf(ps.endCtlParameter().
                    orElse(buildDate()));
            this.startDt = String.valueOf(ps.startCtlParameter()
                    .orElse(DEFAULT_START_DT));
        } else {
            LocalDate lastStatistic = ps.getLastStatistic(LAST_LOAD_START, statistic -> LocalDate.parse(statistic.value));
            this.lastAggregationDate = Date.valueOf(lastStatistic);
        }
        tableForSchema = ps.parentDatamartName().tableName();
    }

    @Override
    public Dataset<Row> buildDatamart() {
        final String tableName = "sbol_logon";
        String sourceTableName = tableForSchema.replace("sbol_logon_aggr_dt", "");
        String sourceSchemaName = tableForSchema.contains("sbol_logon_aggr_dt_ikfl")
                ? tableForSchema.replace("sbol_logon_aggr_dt_ikfl", "").replace("_", "")
                : tableForSchema;
        sourceSchema = sourceSchemaName.length() > 0 ? sourceSchemaName : "1";
        Dataset<Row> sbolLogon = sourceTable(FullTableName.of(newSourceShema, tableName.concat(sourceTableName)));

        final Dataset<Row> sbolLogonLauncher=sourceTable(FullTableName.of(newSourceShema, "sbol_logon_launcher"))
                .where(col("type").equalTo(TYPE).and((lower(col("id_instance"))).equalTo(sourceSchema)
                        .or((coalesce(col("id_instance").cast(IntegerType), lit(0)).equalTo(0)).and(lit(sourceSchema).equalTo("gf"))))).distinct();

        Dataset<Row> ikfl = getLoginsByIkfl(IKFL);
        Dataset<Row> ikfl2 = getLoginsByIkfl(IKFL2);
        Dataset<Row> ikfl3 = getLoginsByIkfl(IKFL3);
        Dataset<Row> ikfl4 = getLoginsByIkfl(IKFL4);
        Dataset<Row> ikfl5 = getLoginsByIkfl(IKFL5);
        Dataset<Row> ikfl6 = getLoginsByIkfl(IKFL6);
        Dataset<Row> ikfl7 = getLoginsByIkfl(IKFL7);
        Dataset<Row> ikflGf = getLoginsByIkfl(IKFL_GF);

        Dataset<Row> logins = SparkSQLUtil.unionAll(ikfl, ikfl2, ikfl3, ikfl4, ikfl5, ikfl6, ikfl7, ikflGf);

        sbolLogon = sbolLogon
                .join(logins, sbolLogon.col("login_id").equalTo(logins.col("id")), "left")
                .select(
                        sbolLogon.col("*"),
                        logins.col("tb_nmb"),
                        logins.col("vsp_nmb"),
                        logins.col("osb_nmb"));


        Dataset<Row> filteredLogon;
        if (isFirstLoading || reload) {
            filteredLogon = sbolLogon
                    .where(
                            col("logon_dt").lt(startDay())
                            .and(col("logon_dt").geq(startTime))
                            .and(col("logon_dt").lt(endTime))
                            .and(col("day_part").geq(startDt)));
            filteredLogon = newSource.equals("0") ? filteredLogon : SparkSQLUtil.unionAll(filteredLogon.where(
                            col("day_part").lt(lit(migrationDate)).or((col("day_part").geq(lit(migrationDate))).and(col("channel_type").isin(CHANNELS)))),
                    getStruct(sbolLogonLauncher.where(col("day_part").geq(startDt)
                            .and(col("logon_dt").lt(endTime)).and(col("logon_dt").geq(startTime)).and(col("day_part").geq(lit(migrationDate))))));

        } else {
            Dataset<Row> logonWithFilteredDayPart = sbolLogon
                    .where(sbolLogon.col("day_part").gt(date_format(lit(lastAggregationDate), DAY_PART_FORMAT)));
            final Dataset<Row> logonWithFilteredDayPartSource =
            newSource.equals("0") ?
                    logonWithFilteredDayPart:
                    SparkSQLUtil.unionAll(logonWithFilteredDayPart.where(
                            col("day_part").lt(lit(migrationDate)).or((col("day_part").geq(lit(migrationDate))).and(col("channel_type").isin(CHANNELS)))),
                            getStruct(sbolLogonLauncher).where(col("day_part").gt(date_format(lit(lastAggregationDate), DAY_PART_FORMAT)).and((col("timestampcolumn")).gt(date_format(lit(lastAggregationDate), DAY_PART_FORMAT))).and(col("day_part").geq(lit(migrationDate)))));

            final Dataset<Row> logonWithFilteredDayPartSaved = saveTemp(
                    logonWithFilteredDayPartSource, "logonWithFilteredDayPart");

            if (isEmpty(logonWithFilteredDayPartSaved)) {
                log.warn("Filtered dataframe is emty. Default statistics disabled. Build is stopped.");
                disableDefaultStatistics();
                return DataFrameUtils.createDF(dc.context(), SBOL_LOGON_AGGR_DT_SCHEMA);
            }

            filteredLogon = logonWithFilteredDayPartSaved
                    .where(logonWithFilteredDayPartSaved.col("logon_dt").gt(lastAggregationDate)
                            .and(logonWithFilteredDayPartSaved.col("logon_dt").lt(startDay())));
        }

        final Dataset<Row> filteredSavedLogon = saveTemp(filteredLogon, "filtered");
        Object maxDate = findMaxDate(filteredSavedLogon.select(col("day_part"), col("logon_dt")));
        if (maxDate == null) {
            log.warn("Table {} is empty after filer 'logon_dt <= {}'", tableName, startDay());
        } else {
            addStatistic(LAST_LOAD_START, maxDate.toString());
        }

        final Dataset<Row> aggrLogons = filteredSavedLogon
                .withColumn("epk_id", coalesce(filteredSavedLogon.col("epk_id").cast(LongType), lit(-1L)))
                .groupBy(
                        col("login_id"),
                        from_unixtime(unix_timestamp(col("logon_dt")), "yyyy-MM-dd 00:00:00").cast(TimestampType).as("logon_dt"),
                        col("application"),
                        col("channel_type"),
                        col("device_info")
                ).agg(
                        min(col("logon_dt")).as("min_logon_dt"),
                        max(col("logon_dt")).as("max_logon_dt"),
                        max(col("epk_id")).as("epk_id"),
                        max(col("tb_nmb")).as("tb_nmb"),
                        max(col("vsp_nmb")).as("vsp_nmb"),
                        max(col("osb_nmb")).as("osb_nmb"),
                        count("*").as("cnt_logon")
                ).select(
                        col("*"),
                        date_format(col("logon_dt"), DAY_PART_FORMAT).as(DAY_PART)
                );


        return aggrLogons
                .withColumn("schema", lit(this.sourceSchema));
    }

    private Object findMaxDate(Dataset<Row> sbolLogon) {
        return sbolLogon
                .where(sbolLogon.col("day_part").lt(startOfDay(buildDate()).toString().substring(0, 10)).and(col("day_part").geq(lit(migrationDate))))
                .select(to_date(max(sbolLogon.col("logon_dt"))).as("max_date"))
                .collectAsList()
                .get(0)
                .getAs("max_date");
    }

    private Dataset<Row> getLoginsByIkfl(DbNameImpl dbName) {
        Dataset<Row> ikfl = sourceTable(dbName.resolve("logins"));
        return ikfl.select(
                col("id"),
                col("last_logon_card_tb").as("tb_nmb"),
                col("last_logon_card_osb").as("osb_nmb"),
                col("last_logon_card_vsp").as("vsp_nmb")
        );
    }
    Dataset<Row> getStruct(Dataset<Row> dataset)
    {
        return dataset.select(
                conv(substring(md5(col("id")), -15, 15), 16, 10).cast(DECIMAL_38_0).as("id"),
                col("logon_id").cast(DECIMAL_38_0).as("login_id"),
                col("logon_dt").cast(TimestampType),
                lit(null).cast(StringType).as("application"),
                col("device_info"),
                col("epk_id").cast(LongType).as("epk_id"),
                col("ctl_loading"),
                col("ctl_validfrom"),
                col("ctl_action"),
                col("channel_type"),
                substring(col("logon_dt"), 0, 10).cast(StringType).as("day_part"),
                col("day_part").cast(StringType).as("timestampcolumn"),
                coalesce(col("id_instance").cast(IntegerType), lit(0)).as("id_instance"),
                concat(lit("IKFL"), when(col("id_instance").equalTo("1"), "").when(col("id_instance").equalTo("GF"), concat(lit("_"), col("id_instance"))).otherwise(col("id_instance"))).as("name_instance"),
                col("tb_nmb"),
                col("vsp_nmb"),
                col("osb_nmb")
        );
    }

    public static void main(String[] args) {
        runner().run(SbolLogonAggrDt.class);
    }
}

