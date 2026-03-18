package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

public interface DataFrameSaver {
    void insertOverwriteTable(FullTableName tableName, Dataset<Row> dataFrame);

    void insertOverwriteTable(FullTableName tableName, Dataset<Row> dataFrame, PartitionInfo partitionInfo);

    void insertIntoTable(FullTableName tableName, Dataset<Row> dataFrame, PartitionInfo partitionInfo);
}


