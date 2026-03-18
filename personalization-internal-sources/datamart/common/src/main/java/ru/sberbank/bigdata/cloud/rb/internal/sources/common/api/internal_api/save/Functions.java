package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;

public class Functions {
    private static final Column NULL_VALUE_PARTITION = lit("NO_DATA_FOR_PARTITION");

    static Dataset<Row> replaceNullsInPartition(Dataset<Row> dataFrame, PartitionInfo partitionInfo) {
        //hive doesn't support partitions which based on column with NULL value, so all nulls should be replaced with special string
        if (partitionInfo != null && partitionInfo.isDynamic()) {
            for (String partitionColumnName : partitionInfo.colNames()) {
                dataFrame = dataFrame.withColumn(partitionColumnName, coalesce(dataFrame.col(partitionColumnName), NULL_VALUE_PARTITION));
            }
        }
        return dataFrame;
    }
}
