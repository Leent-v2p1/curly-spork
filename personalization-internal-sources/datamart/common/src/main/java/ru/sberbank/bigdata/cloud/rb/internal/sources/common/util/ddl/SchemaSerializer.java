package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.ddl;

import org.apache.spark.sql.types.StructType;

public interface SchemaSerializer {

    String serialize(String tableName, StructType schema);
}
