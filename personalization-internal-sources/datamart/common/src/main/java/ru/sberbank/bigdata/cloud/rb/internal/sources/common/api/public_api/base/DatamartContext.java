package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.TableName;

import java.util.List;

/**
 * Interface for loading tables
 */
public interface DatamartContext {

    /**
     * @param name name of table to be loaded from source schema
     *
     * @return corresponding data frame
     */
    Dataset<Row> sourceTable(TableName name);

    Dataset<Row> sourceTable(FullTableName fullTableName);

    Dataset<Row> sourceDiffTable(TableName name);

    Dataset<Row> sourceSparkTable(TableName name);

    /**
     * @param name name of table to be loaded from target schema
     *
     * @return corresponding data frame
     */
    Dataset<Row> targetTable(String name);

    Dataset<Row> targetTableWithPostfix(String name);

    Dataset<Row> stageTable(String name);

    Dataset<Row> stageTableWithPostfix(String name);

    /**
     * @return true if datamart exists, false otherwise
     */
    boolean exists();

    /**
     * @return actual partition of before-built datamart
     */
    Dataset<Row> loadExisting();

    /**
     * @return Sql context
     */
    SparkSession context();

    boolean exists(FullTableName fullTableName);

    List<String> schemaTables(String schema);
}
