package ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.internal;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.ToDeleteDir;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.List;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.CollectionUtils.subtract;

public class ReservingPartitionUpdater {
    private static final Logger log = LoggerFactory.getLogger(ReservingPartitionUpdater.class);
    private final MetastoreService metastoreService;
    private final SparkSession context;
    private final DatamartNaming naming;
    private final String datamartPath;
    private final String historyDatamartPath;
    private final boolean testPartiallyHistoryUpdate;

    public ReservingPartitionUpdater(MetastoreService metastoreService,
                                     SparkSession context,
                                     DatamartNaming naming,
                                     String datamartPath,
                                     String historyDatamartPath,
                                     boolean testPartiallyHistoryUpdate) {
        this.metastoreService = metastoreService;
        this.context = context;
        this.naming = naming;
        this.datamartPath = datamartPath;
        this.historyDatamartPath = historyDatamartPath;
        this.testPartiallyHistoryUpdate = testPartiallyHistoryUpdate;
    }

    public void updatePartitions(ToDeleteDir dirToDelete) {
        List<String> newPartitions = SparkSQLUtil.getPartitionList(context, naming.reserveFullTableName());
        log.info("newPartitions = {}", newPartitions);

        List<String> currentPartitions = SparkSQLUtil.getPartitionList(context, naming.fullTableName());
        log.info("currentPartitions = {}", currentPartitions);

        if (dirToDelete == ToDeleteDir.ALL || dirToDelete == ToDeleteDir.ACTUAL) {
            addNewActualPartitions(newPartitions, currentPartitions);
            if (!testPartiallyHistoryUpdate) {
                deleteActualPartitions(newPartitions, currentPartitions);
            }
            addNewHistPartitions();
        } else if (dirToDelete == ToDeleteDir.NONE) {
            addNewActualPartitions(newPartitions, currentPartitions);
            addNewHistPartitions();
        } else {
            throw new IllegalStateException("Cannot process enum " + dirToDelete);
        }
    }

    private void addNewActualPartitions(List<String> newPartitions, List<String> currentPartitions) {
        addNewPartitions(newPartitions, currentPartitions, naming.fullTableName(), datamartPath);
    }

    private void deleteActualPartitions(List<String> newPartitions, List<String> currentPartitions) {
        deleteStalePartitions(newPartitions, currentPartitions);
    }

    private void addNewHistPartitions() {
        if (historyReserveTableExists()) {
            List<String> currentHistPartitions = SparkSQLUtil.getPartitionList(context, naming.historyFullTableName());
            log.info("currentHistPartitions: {}", currentHistPartitions);

            List<String> newHistPartitions = SparkSQLUtil.getPartitionList(context, naming.historyReserveFullTableName());
            log.info("histPartitions: {}", newHistPartitions);

            addNewPartitions(newHistPartitions, currentHistPartitions, naming.historyFullTableName(), historyDatamartPath);
        }
    }

    private void deleteStalePartitions(List<String> newPartitions, List<String> currentPartitions) {
        // получаем список партиций для удаления (которые есть в текущей витрины, но нет в зарезервированной витрине)
        final List<String> stalePartitions = subtract(currentPartitions, newPartitions);
        log.info("remove partitions: {}", stalePartitions);

        final FullTableName fullTableName = FullTableName.of(naming.fullTableName());
        for (String partition : stalePartitions) {
            metastoreService.dropPartition(fullTableName, partition);
        }
    }

    private void addNewPartitions(List<String> newPartitions, List<String> currentPartitions, String fullTableName, String datamartPath) {
        final List<String> partitionsToAdd = subtract(newPartitions, currentPartitions);
        log.info("Partitions to add: {}", partitionsToAdd);
        for (String partitionToAdd : partitionsToAdd) {
            metastoreService.addPartition(FullTableName.of(fullTableName), partitionToAdd, datamartPath);
        }
    }

    private boolean historyReserveTableExists() {
        FullTableName historyReserve = FullTableName.of(naming.historyReserveFullTableName());
        return metastoreService.tableExists(historyReserve);
    }
}
