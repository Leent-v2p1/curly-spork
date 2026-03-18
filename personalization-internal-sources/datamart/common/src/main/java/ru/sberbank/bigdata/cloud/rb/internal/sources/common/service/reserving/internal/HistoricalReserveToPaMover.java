package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.internal;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.auto_config.BuildRequiredChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save.TableSaveHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.CtlDefaultStatistics;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.ctl.statistics.StatisticToFileWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.StatTableWriter;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.List;

/**
 * Перемещает данные исторической витрины из stg в pa и обновляет состояние в hive metastore.
 */
public class HistoricalReserveToPaMover extends ReserveToPaMover {

    private static final Logger log = LoggerFactory.getLogger(HistoricalReserveToPaMover.class);

    public HistoricalReserveToPaMover(SparkSession context,
                                      DatamartNaming naming,
                                      IncrementSaveRemover incrementSaveRemover,
                                      BuildRequiredChecker buildRequiredChecker,
                                      StatisticToFileWriter statisticToFileWriter,
                                      CtlDefaultStatistics ctlDefaultStatistics,
                                      StatTableWriter statTableWriter,
                                      ReservingParameters parameters) {
        super(context,
                naming,
                incrementSaveRemover,
                buildRequiredChecker,
                statisticToFileWriter,
                ctlDefaultStatistics,
                statTableWriter, parameters
        );
    }

    @Override
    public void dropPaIfFirstLoading() {
        super.dropPaIfFirstLoading();
        if (firstLoading) {
            final String fullTableName = naming.historyFullTableName();
            log.info("Dropping history table (if exist) for first loading: {}", fullTableName);
            SparkSQLUtil.dropTable(context, fullTableName);
        }
    }

    @Override
    public void createTableIfNotExist() {
        super.createTableIfNotExist();
        if (isHistoryReserveTableExists()) {
            final String historyFullTableName = naming.historyFullTableName();
            log.info("Creating history table (if not exist): {}", historyFullTableName);
            new TableSaveHelper(context).createCopyTable(naming.historyReserveFullTableName(), historyFullTableName);
        }
    }

    @Override
    public void processDirsToDelete() {
        if (dirToDelete.equals(ToDeleteDir.ACTUAL)) {
            if (testPartiallyHistoryUpdate) {
                log.info("using partiallyHistoryUpdate");
                final List<String> reserveSnpPartitions = SparkSQLUtil.getPartitionList(context, naming.reserveFullTableName());
                for (String reserveSnpPartition : reserveSnpPartitions) {
                    metastoreService.dropPartition(FullTableName.of(naming.fullTableName()), reserveSnpPartition);
                }
            } else {
                HDFSHelper.deleteDirectory(datamartPath);
            }
        } else {
            throw new IllegalStateException("ToDeleteDir for historical datamart " + naming.fullTableName() + " should be ToDeleteDir.ACTUAL");
        }
    }

    @Override
    public void moveFiles() {
        super.moveFiles();

        final FullTableName histReserveFullTableName = FullTableName.of(naming.historyReserveFullTableName());
        final String stgDatabaseLocation = SparkSQLUtil.databaseLocation(context, histReserveFullTableName.dbName());
        final String histReservedPath = stgDatabaseLocation + "/" + histReserveFullTableName.tableName();
        log.info("Moving historical files from: {} to: {}", histReservedPath, histDatamartPath);
        HDFSHelper.moveFiles(histReservedPath, histDatamartPath);
    }

    @Override
    public void dropReserving() {
        super.dropReserving();
        if (isHistoryReserveTableExists()) {
            final String historyReserveFullTableName = naming.historyReserveFullTableName();
            log.info("Dropping history reserving table {}", historyReserveFullTableName);
            SparkSQLUtil.dropTable(context, historyReserveFullTableName);
        }
    }

    private boolean isHistoryReserveTableExists() {
        return metastoreService.tableExists(FullTableName.of(naming.historyReserveFullTableName()));
    }
}
