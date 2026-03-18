package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.logging.LoggerTypeId;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.YamlPropertiesParser;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.Replica;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.replica.ReplicaContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableNameImpl;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.SysPropertyTool;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaActionDependency;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration.ReplicaConf;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.CTL_VALID_FROM;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.CTL_VALID_TO;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions.HST_POSTFIX;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil.unionAll;

/**
 * Восстанавливает реплику на дату recoveryDate
 */
public class RecoveredReplicaTableCreator extends Replica {

    private static final Logger log = LoggerFactory.getLogger(RecoveredReplicaTableCreator.class);
    private static final Set<String> sourcesFromOds = Collections.singleton("internal_mdm_mdm_hist");
    private static final String HISTORY_SNAPSHOT_DATE_FORMAT = "yyyyMMddHHmmss";

    private final FullTableName replicaSnapshotTable;
    private final FullTableName replicaHistoryTable;
    private final LocalDate recoveryDate;

    RecoveredReplicaTableCreator(ReplicaContext rc,
                                 FullTableName replicaSnapshotTable,
                                 FullTableName replicaHistoryTable,
                                 LocalDate recoveryDate) {
        super(rc);
        this.replicaSnapshotTable = replicaSnapshotTable;
        this.replicaHistoryTable = replicaHistoryTable;
        this.recoveryDate = recoveryDate;
    }

    public void run() {
        replicaContext.save(buildReplica());
    }

    @Override
    public Dataset<Row> buildReplica() {
        final Timestamp recoveryDateTs = Timestamp.valueOf(recoveryDate.atStartOfDay());

        final Dataset<Row> snpTable = sourceReplicaTable(replicaSnapshotTable);
        final Dataset<Row> snpPart = snpTable.where(snpTable.col(CTL_VALID_FROM).leq(recoveryDateTs));

        final FullTableName historyTableName = selectHistoryTable();
        final String fullTableName = historyTableName.fullTableName();
        log.info("Selected table - {}", fullTableName);
        final Dataset<Row> histTable = sourceReplicaTable(historyTableName);
        final Dataset<Row> histPart = histTable
                .where(histTable.col(CTL_VALID_FROM).leq(recoveryDateTs)
                        .and(histTable.col(CTL_VALID_TO).geq(recoveryDateTs)));

        return unionAll(snpPart, histPart);
    }

    private FullTableName selectHistoryTable() {
        final String dbName = replicaHistoryTable.dbName();
        final boolean isFromOds = sourcesFromOds.stream().anyMatch(sourceFromOds -> sourceFromOds.equals(dbName));
        if (isFromOds) {
            final FullTableName histTableName = new FullTableNameImpl(dbName, replicaHistoryTable.tableName()
                    .replaceAll(HST_POSTFIX + "$", ""));
            log.info("Source {} from ods, selected 'hist' table is {}", dbName, histTableName);
            return histTableName;
        }
        final List<String> tablesInDb = schemaTables(dbName);
        return tablesInDb
                .stream()
                .filter(tableName -> tableName.matches(replicaHistoryTable.tableName() + "_\\d.*$"))
                .map(tableName -> FullTableName.of(dbName, tableName))
                .peek(tableName -> log.info("Selected table for {} - {}", replicaHistoryTable.tableName(), tableName))
                .map(table -> new HistoryTableSelector.TableAndTime(getCtlValidFrom(table), table))
                .max(Comparator.comparing(tableAndTime -> tableAndTime.time))
                .orElseThrow(() -> new IllegalStateException("No history table " + replicaHistoryTable.fullTableName() + " for day " + recoveryDate))
                .tableName;
    }

    private LocalDateTime getCtlValidFrom(FullTableName tableName) {
        final String fullTableName = tableName.fullTableName();
        final String[] nameParts = fullTableName.split("_");
        if (nameParts.length < 2) {
            throw new IllegalArgumentException("Wrong history source table name format: " + fullTableName
                    + ", format should be 'tableName_loadingId_yyyyMMddHHmmss'");
        }

        final String timestamp = nameParts[nameParts.length - 1];
        final LocalDateTime ctlValidFrom;
        if (timestamp.length() == HISTORY_SNAPSHOT_DATE_FORMAT.length()) {
            ctlValidFrom = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern(HISTORY_SNAPSHOT_DATE_FORMAT));
        } else {
            //old format of table name - 'tableName_loadingId_epochSeconds'
            ctlValidFrom = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.valueOf(timestamp)), ZoneId.systemDefault());
        }

        return ctlValidFrom;
    }

    public static void main(String[] args) {
        final String datamartHistoryTableName = SysPropertyTool.safeSystemProperty("spark.datamart.history.table.name");
        LoggerTypeId.set(datamartHistoryTableName);
        final String parentHistoryTableName = SysPropertyTool.safeSystemProperty("spark.datamart.parent.table.name");
        final ReplicaActionDependency replicaActionDependency = new ReplicaActionDependency(new YamlPropertiesParser().parse("/replica-properties.yaml"));
        final Map<String, ReplicaConf> replicaConfigs = replicaActionDependency.resolve();
        final HistoryDatamartServiceFactory datamartServiceFactory = new HistoryDatamartServiceFactory(replicaConfigs, FullTableName.of(datamartHistoryTableName));
        final LocalDate recoveryDate = datamartServiceFactory.recoveryDate()
                .orElseThrow(() -> new IllegalStateException("'recoveryDate' system property is required"));

        new RecoveredReplicaTableCreator(
                datamartServiceFactory.replicaOnDateContext(FullTableName.of(parentHistoryTableName)),
                datamartServiceFactory.replicaSnapshotTable(),
                datamartServiceFactory.replicaHistoryTable(),
                recoveryDate
        ).run();
    }
}
