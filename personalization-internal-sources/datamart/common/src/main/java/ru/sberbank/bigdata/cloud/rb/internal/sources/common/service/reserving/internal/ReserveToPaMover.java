package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.internal;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.BuildRequiredChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerJournal;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlDefaultStatistics;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticToFileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.StatTableWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.PathBuilder;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.BUSINESS_DATE_STAT_ID;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.CSN;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticId.MONTH_WF_SCHEDULE;

/**
 * Перемещает данные неисторической витрины из stg в pa и обновляет состояние в hive metastore.
 */
public class ReserveToPaMover {

    private static final Logger log = LoggerFactory.getLogger(ReserveToPaMover.class);
    //services
    protected final SparkSession context;
    protected final DatamartNaming naming;
    protected final IncrementSaveRemover incrementSaveRemover;
    protected final MetastoreService metastoreService;
    protected final BuildRequiredChecker buildRequiredChecker;
    protected final StatisticToFileWriter statisticToFileWriter;
    protected final CtlDefaultStatistics ctlDefaultStatistics;
    protected final StatTableWriter statTableWriter;

    //parameters
    protected final String datamartPath;
    protected final String histDatamartPath;
    protected final ToDeleteDir dirToDelete;
    protected final Optional<LocalDate> recoveryDate;
    protected final LocalDate buildDate;
    protected final boolean recoveryMode;
    protected final boolean firstLoading;
    protected final boolean testPartiallyHistoryUpdate;
    protected final boolean isRepartitionNeeded;

    public ReserveToPaMover(SparkSession context,
                            DatamartNaming naming,
                            IncrementSaveRemover incrementSaveRemover,
                            BuildRequiredChecker buildRequiredChecker,
                            StatisticToFileWriter statisticToFileWriter,
                            CtlDefaultStatistics ctlDefaultStatistics,
                            StatTableWriter statTableWriter,
                            ReservingParameters parameters) {
        //services
        this.context = context;
        this.naming = naming;
        this.incrementSaveRemover = incrementSaveRemover;
        this.metastoreService = new MetastoreService(context);
        this.buildRequiredChecker = buildRequiredChecker;
        this.statisticToFileWriter = statisticToFileWriter;
        this.ctlDefaultStatistics = ctlDefaultStatistics;
        this.statTableWriter = statTableWriter;
        //parameters
        this.datamartPath = PathBuilder.hdfsTablePath(naming.resultSchema(), naming.resultTable(), context);
        this.histDatamartPath = PathBuilder.hdfsTablePath(naming.resultSchema(), naming.histTableName(), context);
        this.dirToDelete = parameters.getDirToDelete();
        this.recoveryDate = parameters.getRecoveryDate();
        this.recoveryMode = parameters.isRecoveryMode();
        this.firstLoading = parameters.isFirstLoading();
        this.testPartiallyHistoryUpdate = parameters.isTestPartiallyHistoryUpdate();
        this.isRepartitionNeeded = parameters.isRepartitionNeeded();
        this.buildDate = parameters.getBuildDate();
    }

    public static ReservingBuilder builder(DatamartServiceFactory serviceFactory) {
        return new ReservingBuilder(serviceFactory);
    }

    public void moveReserveToPa() {
        if (!isReservingRequired()) {
            return;
        }
        dropPaIfFirstLoading();
        createTableIfNotExist();
        applyIncrementSafeRemover();
        processDirsToDelete();
        updatePartitions();
        moveFiles();
        auditReserving();
        dropReserving();
        writeStatistics();
        updateStatTable();
    }

    protected void auditReserving() {
        List<String> PartitionsList = SparkSQLUtil.getPartitionList(context, FullTableName.of(resolveFullTableName()));
        String Partitions = String.join(",", PartitionsList);
        Datamart.moveAppLogDm(Partitions);
    }

    protected void updateStatTable() {
        statTableWriter.writeStats();
    }

    protected void dropPaIfFirstLoading() {
        if (firstLoading) {
            final String fullTableName = naming.fullTableName();
            log.info("Dropping pa table (if exist) for first loading: {}", fullTableName);
            SparkSQLUtil.dropTable(context, fullTableName);
        }
    }

    protected boolean isReservingRequired() {
        if (!buildRequiredChecker.isDatamartBuildingNeeded()) {
            log.info("There is no need of ReservingService because datamart already has been built successfully today");
            return false;
        }
        String fullTableName = naming.fullTableName();
        log.info("ReservingService for {} is required ", fullTableName);
        return true;
    }

    public void createTableIfNotExist() {
        String fullTableName = naming.fullTableName();
        log.info("Creating table (if not exist): {}", fullTableName);
        new TableSaveHelper(context).createCopyTable(naming.reserveFullTableName(), fullTableName);
    }

    public void applyIncrementSafeRemover() {
        log.info("Applying incrementSaveRemover");
        if (incrementSaveRemover != null) {
            incrementSaveRemover.remove(context, naming, buildDate, firstLoading);
        }
    }

    public void processDirsToDelete() {
        log.info("Processing dirs to delete: {}", dirToDelete);
        switch (dirToDelete) {
            case ALL:
                HDFSHelper.deleteDirectory(datamartPath);
                break;
            case NONE:
                break;
            case ACTUAL:
                throw new IllegalStateException("ToDeleteDir.ACTUAL is not supported by nonhistorical datamart " + naming.fullTableName());
        }
    }

    public void updatePartitions() {
        log.info("updating partitions");
        new ReservingPartitionUpdater(metastoreService, context, naming, datamartPath, histDatamartPath, testPartiallyHistoryUpdate).updatePartitions(dirToDelete);
    }

    public void moveFiles() {
        final FullTableName fullTableName = FullTableName.of(resolveFullTableName());
        final String reserveDatamartPath = PathBuilder.hdfsTablePath(fullTableName.dbName(), fullTableName.tableName(), context);
        log.info("Moving files from: {} to: {}", reserveDatamartPath, datamartPath);
        HDFSHelper.moveFiles(reserveDatamartPath, datamartPath);
    }

    public void dropReserving() {
        final String reserve = naming.reserveFullTableName();
        log.info("Dropping reserving table {}", reserve);
        SparkSQLUtil.dropTable(context, reserve);
        if (isRepartitionNeeded) {
            final String repartition = naming.repartitionedFullTableName();
            log.info("Dropping repartitioned table {}", repartition);
            SparkSQLUtil.dropTable(context, repartition);
        }
    }

    public void writeStatistics() {
        log.info("Publishing statistics");
        Map<StatisticId, String> statisticsToPublish = ctlDefaultStatistics.defaultStatistics();
        if (recoveryMode) {
            log.warn("recoveryMode is on. override default statistics");
            statisticsToPublish.remove(CSN);
            statisticsToPublish.remove(MONTH_WF_SCHEDULE);
            LocalDate recoveryDateValue = recoveryDate.orElseThrow(() -> new IllegalStateException("recoveryDate shouldn't be empty"));
            String previous = statisticsToPublish.put(BUSINESS_DATE_STAT_ID, recoveryDateValue.format(ISO_LOCAL_DATE));
            log.info("override default value for {}, previous value is {}", BUSINESS_DATE_STAT_ID, previous);
        }
        log.info("statistics send to file: {}", statisticsToPublish);
        statisticsToPublish.forEach(statisticToFileWriter::writeStatistic);
    }

    public String resolveFullTableName() {
        return isRepartitionNeeded ? naming.repartitionedFullTableName() : naming.reserveFullTableName();
    }
}
