package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataFix {
    Dataset<Row> apply(Dataset<Row> dataFrame);

    DataFixType type();
}
