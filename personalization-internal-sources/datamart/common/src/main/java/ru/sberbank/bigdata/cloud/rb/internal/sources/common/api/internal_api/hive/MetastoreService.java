package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.annotation.VisibleForTesting;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.functions.CollectionUtils.extractExactlyOneElement;

public class MetastoreService {
    private static final Logger log = LoggerFactory.getLogger(MetastoreService.class);
    private final SparkSession sqlContext;

    public MetastoreService(SparkSession sqlContext) {
        this.sqlContext = sqlContext;
    }

    public boolean tableExists(FullTableName fullTableName) {
        final boolean tableExists = sqlContext.sqlContext().tables(fullTableName.dbName())
                .where("isTemporary = false AND tableName = '" + fullTableName.tableName() + "'")
                .count() == 1;
        log.info("Table {} exists: {}", fullTableName, tableExists);
        return tableExists;
    }

    /**
     * @param partition - string in format: part_col_1=part_val_1/part_col_2=part_val_2
     */
    @VisibleForTesting
    protected Optional<String> partitionLocation(FullTableName table, String partition) {
        try {
            final String query = "DESCRIBE FORMATTED " + table.fullTableName() + " PARTITION (" + formatPartition(partition) + ")";
            log.info("Executing query: {}", query);
            final List<Row> rows = sqlContext.sql(query).collectAsList();
            log.info("Result of partitionLocation query: {}", rows);

            return Optional.of(extractExactlyOneElement(
                    rows.stream()
                            .filter(row -> row.getString(0).startsWith("Location"))
                            .map(row -> row.getString(1))
                            .filter(row -> row.contains(partition.replace("'", "")))
                            .collect(toList())));
        } catch (Exception e) {
            if (e instanceof NoSuchPartitionException) {
                return Optional.empty();
            } else {
                throw e;
            }
        }
    }

    /**
     * @param partitionInfo - partitionInfo should be nonDynamic
     */
    public Optional<String> partitionLocation(FullTableName table, PartitionInfo partitionInfo) {
        return partitionLocation(table, partitionInfo.toPath());
    }

    /**
     * @param partition string in format: part_col_1=part_val_1/part_col_2=part_val_2
     */
    public void addPartition(FullTableName fullTableName, String partition, String tablePath) {
        String query = String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s) LOCATION '%s/%s'",
                fullTableName.fullTableName(),
                formatPartition(partition),
                tablePath,
                partition);
        log.info("alterTable: {}", query);
        sqlContext.sql(query);
    }

    public void addPartition(FullTableName fullTableName, PartitionInfo partitionInfo) {
        if (partitionInfo.isDynamic()) {
            throw new IllegalStateException("Partition should be non dynamic");
        }
        final String tableLocation = SparkSQLUtil.tableLocation(sqlContext, fullTableName.fullTableName())
                .orElseThrow(() -> new IllegalStateException("Cannot find location for table " + fullTableName));

        addPartition(fullTableName, partitionInfo.toPath(), tableLocation);
    }

    /**
     * @param partition string in format: part_col_1=part_val_1/part_col_2=part_val_2
     */
    public void dropPartition(FullTableName tableName, String partition) {
        partitionLocation(tableName, partition)
                .ifPresent(HDFSHelper::deleteDirectory);
        log.info("Drop partition of table {} - {}", tableName, partition);
        sqlContext.sql("ALTER TABLE " + tableName.fullTableName() + " DROP IF EXISTS PARTITION (" + formatPartition(partition) + ")");
    }

    private String formatPartition(String partitionExpr) {
        return Arrays.stream(partitionExpr.split("/"))
                .map(partition -> partition.split("="))
                .map(keyValue -> String.format("%s = '%s'", keyValue[0], keyValue[1]))
                .collect(joining(", "));
    }

    public List<PartitionInfo> getPartitions(FullTableName fullTableName) {
        //SparkSQLUtil.recoverPartitions(sqlContext, fullTableName);
        return SparkSQLUtil.getPartitions(sqlContext, fullTableName);
    }

    public Long getMaxLoadingPart(FullTableName tableName, String partColumn) {
        final List<PartitionInfo> partitions = getPartitions(tableName);
        return partitions.stream()
                .flatMap(partitionInfo -> partitionInfo.partitions().stream())
                .filter(partition -> partColumn.equals(partition.name) && partition.value != null)
                .map(partition -> Long.valueOf(partition.value))
                .max(Long::compareTo)
                .orElseThrow(() -> new IllegalStateException("Can't find lastPartition of " + tableName));
    }
}
