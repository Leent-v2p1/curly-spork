package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.ExtraStatisticAccumulator;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.StringReplacer;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixAPI;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerJournal;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.DatamartSaver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TempSaver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DefaultDatamartFields;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.DbName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticToFileWriter;

import java.sql.Timestamp;
import java.time.LocalDate;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.*;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.date.DateHelper.*;

public abstract class Datamart {

    private static final Logger log = LoggerFactory.getLogger(Datamart.class);

    protected DatamartContext dc;
    protected TempSaver tempSaver;
    protected DatamartSaver datamartSaver;
    protected StringReplacer emptyStringReplacer;
    protected StatisticToFileWriter statisticToFileWriter;
    protected ExtraStatisticAccumulator extraStatisticAccumulator;
    protected ExtraStatisticAccumulator disabledStatisticAccumulator;
    protected DataFixAPI dataFixApi;
    protected MetastoreService metastoreService;
    //parameters
    protected boolean isFirstLoading;
    protected boolean isReload;
    private DefaultDatamartFields defaultDatamartFields;
    private LocalDate buildDateValue;

    /**
     * Метод для конфигурации витрины или переопределения параметров родительской витрины
     */
    public void init(DatamartServiceFactory datamartServiceFactory) {
    }

    /**
     * Метод для применения датафиксов для витрины. Процедура применения датафиксов совершается c помощью класса SchemaChanger.
     */
    public void addDatafixes() {
    }

    /**
     * Method for configuration of saving strategy
     */
    public HiveSavingStrategy customSavingStrategy(DatamartServiceFactory datamartServiceFactory) {
        return null;
    }

    /**
     * Method
     * - creates datamart dataframe with buildDatamart() method
     * - add default columns
     * - replace empty strings with null
     * - save result dataframe
     * - write to file statistics and its values
     * - write to another file disabled statistic
     */
    public void run() {
        if (!isFirstLoading) {
            addDatafixes();
            dataFixApi.run(this);
        }
        final Dataset<Row> datamart = buildDatamart();
        final Dataset<Row> datamartWithCtlColumns = defaultDatamartFields.addDefault(datamart);
        final Dataset<Row> datamartWithReplacedEmptyStrings = emptyStringReplacer.replaceStrings(datamartWithCtlColumns);
        datamartSaver.save(datamartWithReplacedEmptyStrings);
        writeStatistics();
        disableStatisticsPublishing();
    }

    /**
     * Method to override in inherit datamarts to describe datamart code
     */
    public abstract Dataset<Row> buildDatamart();

    /*Build date and its derivatives*/
    public LocalDate buildDate() {
        return buildDateValue;
    }

    public Timestamp yesterday() {
        return startOfYesterday(buildDate());
    }

    public Timestamp startDay() {
        return startOfDay(buildDate());
    }

    public Timestamp nextDay() {
        return startOfDay(buildDate().plusDays(1));
    }

    public Timestamp firstDayCurrentMonth() {
        return firstDayOfCurrentMonth(buildDate());
    }

    public Timestamp firstDayPreviousMonth() {
        return firstDayOfPreviousMonth(buildDate());
    }

    public Timestamp lastDayPreviousMonth() {
        return lastDayOfPreviousMonth(buildDate());
    }

    /*DatamartContext wrapper methods*/
    public Dataset<Row> sourceTable(TableName name) {
        return dc.sourceTable(name);
    }

    public Dataset<Row> sourceTable(FullTableName fullTableName) {
        return dc.sourceTable(fullTableName);
    }

    public Dataset<Row> sourceTable(DbName schema, TableName name) {
        return dc.sourceTable(FullTableName.of(schema.dbName(), name.tableName()));
    }

    public Dataset<Row> sourceSparkTable(TableName name) {
        return dc.sourceSparkTable(name);
    }

    public Dataset<Row> sourceDiffTable(TableName name) {
        return dc.sourceDiffTable(name);
    }

    public Dataset<Row> targetTable(String name) {
        return dc.targetTable(name);
    }

    public Dataset<Row> targetTableWithPostfix(String name) {
        return dc.targetTableWithPostfix(name);
    }

    public Dataset<Row> stageTable(String name) {
        return dc.stageTable(name);
    }

    protected Dataset<Row> self() {
        return dc.loadExisting();
    }

    protected boolean exists() {
        return dc.exists();
    }

    public Dataset<Row> saveTemp(Dataset<Row> dataFrame, String tempTableName) {
        final Dataset<Row> result = tempSaver.saveTemp(dataFrame, tempTableName);
        log.info("Temporary table {} saved successfully", tempTableName);
        return result;
    }

    /*Statistic publishing*/
    public void addStatistic(StatisticId statId, String value) {
        log.info("adding additional statistic {} with value: {}. Statistic will be published after datamart build", statId, value);
        if (!isReload) {
            extraStatisticAccumulator.addStatistic(statId, value);
        }
    }

    protected void writeStatistics() {
        extraStatisticAccumulator.getStatistics().forEach(statisticToFileWriter::writeStatistic);
    }

    public void disableStatistic(StatisticId statId) {
        log.info("disable statistic {}", statId);
        disabledStatisticAccumulator.addStatistic(statId, "");
    }

    public void disableDefaultStatistics() {
        disableStatistic(BUSINESS_DATE_STAT_ID);
        disableStatistic(CHANGE_STAT_ID);
        disableStatistic(CSN);
        disableStatistic(LAST_LOADED_STAT_ID);
        disableStatistic(MONTH_WF_SCHEDULE);
        disableStatistic(LOADING_TYPE);
    }

    public void disableStatisticsPublishing() {
        disabledStatisticAccumulator.getStatistics()
                .keySet()
                .forEach(statisticToFileWriter::writeDisabledStatistic);
    }

    /*Getters and setters*/

    public DatamartContext getDc() {
        return dc;
    }

    public void setDc(DatamartContext dc) {
        this.dc = dc;
    }

    public TempSaver getTempSaver() {
        return tempSaver;
    }

    public void setTempSaver(TempSaver tempSaver) {
        this.tempSaver = tempSaver;
    }

    public DatamartSaver getDatamartSaver() {
        return datamartSaver;
    }

    public void setDatamartSaver(DatamartSaver datamartSaver) {
        this.datamartSaver = datamartSaver;
    }

    public StringReplacer getEmptyStringReplacer() {
        return emptyStringReplacer;
    }

    public void setEmptyStringReplacer(StringReplacer emptyStringReplacer) {
        this.emptyStringReplacer = emptyStringReplacer;
    }

    public StatisticToFileWriter getStatisticToFileWriter() {
        return statisticToFileWriter;
    }

    public void setStatisticToFileWriter(StatisticToFileWriter statisticToFileWriter) {
        this.statisticToFileWriter = statisticToFileWriter;
    }

    public ExtraStatisticAccumulator getExtraStatisticAccumulator() {
        return extraStatisticAccumulator;
    }

    public void setExtraStatisticAccumulator(ExtraStatisticAccumulator extraStatisticAccumulator) {
        this.extraStatisticAccumulator = extraStatisticAccumulator;
    }

    public ExtraStatisticAccumulator getDisabledStatisticAccumulator() {
        return disabledStatisticAccumulator;
    }

    public void setDisabledStatisticAccumulator(ExtraStatisticAccumulator disabledStatisticAccumulator) {
        this.disabledStatisticAccumulator = disabledStatisticAccumulator;
    }

    public boolean isFirstLoading() {
        return isFirstLoading;
    }


    public boolean isReload() {
        return isReload;
    }

    public void setFirstLoading(boolean firstLoading) {
        isFirstLoading = firstLoading;
    }

    public void setReload(boolean reload) {
        isReload = reload;
    }

    public void setBuildDateValue(LocalDate buildDateValue) {
        this.buildDateValue = buildDateValue;
    }

    public DefaultDatamartFields getDefaultDatamartFields() {
        return defaultDatamartFields;
    }

    public void setDefaultDatamartFields(DefaultDatamartFields defaultDatamartFields) {
        this.defaultDatamartFields = defaultDatamartFields;
    }

    public DataFixAPI getDataFixApi() {
        return dataFixApi;
    }

    public void setDataFixApi(DataFixAPI dataFixApi) {
        this.dataFixApi = dataFixApi;
    }

    public MetastoreService getMetastoreService() {
        return metastoreService;
    }

    public void setMetastoreService(MetastoreService metastoreService) {
        this.metastoreService = metastoreService;
    }

    public static void startAppLogDm() { LoggerJournal.auditStartApp(); }

    public static void stgAppLogDm(Integer ctlLoadingId, Long dirSizeBytes, Long rowsCount) { LoggerJournal.auditStgApp(ctlLoadingId, dirSizeBytes, rowsCount); }

    public static void moveAppLogDm(String partitions) { LoggerJournal.auditMoveApp(partitions); }

    public static void endAppLogDm() { LoggerJournal.auditEndApp(); }
}
