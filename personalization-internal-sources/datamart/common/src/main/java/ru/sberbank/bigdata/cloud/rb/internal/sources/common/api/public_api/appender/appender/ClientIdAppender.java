package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.appender.appender;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface ClientIdAppender {

    Dataset<Row> append(Dataset<Row> toDatamart, String datamartColumnToJoin, String resultMdmFieldName);
}
