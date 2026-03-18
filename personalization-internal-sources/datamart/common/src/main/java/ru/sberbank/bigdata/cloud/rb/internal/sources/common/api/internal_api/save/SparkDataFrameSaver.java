package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.file.HDFSHelper;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.Arrays;
import java.util.List;

public class SparkDataFrameSaver implements DataFrameSaver {
    private static final Logger log = LoggerFactory.getLogger(SparkDataFrameSaver.class);
    private final SparkSession sqlContext;
    private final HDFSHelper hdfsHelper;
    private final MetastoreService metastoreService;

    public SparkDataFrameSaver(SparkSession sqlContext) {
        this.sqlContext = sqlContext;
        this.hdfsHelper = new HDFSHelper();
        this.metastoreService = new MetastoreService(sqlContext);
    }

    @Override
    public void insertOverwriteTable(FullTableName tableName, Dataset<Row> dataFrame) {
        insertIntoTable(tableName, dataFrame, true);
    }

    private void insertIntoTable(FullTableName tableName, Dataset<Row> dataFrame, boolean overwrite) {
        dataFrame = reorderColumnsToMetastore(tableName, dataFrame);
        final String tableLocation = SparkSQLUtil.tableLocation(sqlContext, tableName.fullTableName())
                .orElseThrow(() -> new IllegalStateException("Cannot find location of table " + tableName));
        log.info("Inserting into table - {}, overwrite - {}, in location - {}", tableName, overwrite, tableLocation);
        dataFrame
                .write()
                .format("parquet")
                .mode(overwrite ? SaveMode.Overwrite : SaveMode.Append)
                .save(tableLocation);
        sqlContext.catalog().refreshTable(tableName.fullTableName());
    }

    @Override
    public void insertOverwriteTable(FullTableName tableName, Dataset<Row> dataFrame, PartitionInfo partitionInfo) {
        if (partitionInfo == null) {
            insertOverwriteTable(tableName, dataFrame);
        } else {
            insertIntoTable(tableName, dataFrame, partitionInfo, true);
        }
    }

    @Override
    public void insertIntoTable(FullTableName tableName, Dataset<Row> dataFrame, PartitionInfo partitionInfo) {
        if (partitionInfo == null) {
            insertIntoTable(tableName, dataFrame, false);
        } else {
            insertIntoTable(tableName, dataFrame, partitionInfo, false);
        }
    }

    private void insertIntoTable(FullTableName tableName, Dataset<Row> dataFrame, PartitionInfo partitionInfo, boolean overwrite) {
        final String tableLocation = SparkSQLUtil.tableLocation(sqlContext, tableName.fullTableName())
                .orElseThrow(() -> new IllegalStateException("Cannot find location of table " + tableName));
        log.info("Inserting into table - {}, overwrite - {}, in location - {}, in partition - {}", tableName, overwrite, tableLocation, partitionInfo);
        final SaveMode saveMode = overwrite ? SaveMode.Overwrite : SaveMode.Append;
        if (partitionInfo.isDynamic()) {
            dataFrame = reorderColumnsToMetastore(tableName, dataFrame);

            Functions.replaceNullsInPartition(dataFrame, partitionInfo)
                    .write()
                    .partitionBy(partitionInfo.colNames().toArray(new String[0]))
                    .mode(saveMode)
                    .save(tableLocation);

            final List<String> folderNames = hdfsHelper.folderNamesRecursive(tableLocation);
            folderNames.forEach(folderName -> metastoreService.addPartition(tableName, folderName, tableLocation));
        } else {
            final Column[] metastoreColNames = Arrays.stream(sqlContext.table(tableName.fullTableName()).columns())
                    .filter(colName -> !partitionInfo.colNames().contains(colName))
                    .map(functions::col)
                    .toArray(Column[]::new);
            dataFrame = dataFrame.select(metastoreColNames);
            dataFrame
                    .write()
                    .format("parquet")
                    .mode(saveMode)
                    .save(tableLocation + "/" + partitionInfo.toPath());
            metastoreService.addPartition(tableName, partitionInfo);
        }
        sqlContext.catalog().refreshTable(tableName.fullTableName());
    }

    private Dataset<Row> reorderColumnsToMetastore(FullTableName tableName, Dataset<Row> dataFrame) {
        final Column[] metastoreColumnNames = Arrays.stream(sqlContext.table(tableName.fullTableName()).columns())
                .map(functions::col)
                .toArray(Column[]::new);
        dataFrame = dataFrame.select(metastoreColumnNames);
        return dataFrame;
    }
}
