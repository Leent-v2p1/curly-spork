package ru.sberbank.bigdata.cloud.rb.internal.sources.common.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class DataFrameUtils {

    public static Dataset<Row> createDF(SparkSession sqlContext, StructType structType, Row... rows) {
        return sqlContext.createDataFrame(asList(rows), structType);
    }

    public static Dataset<Row> createDF(SparkSession sqlContext, StructType structType, List<Row> rows) {
        return sqlContext.createDataFrame(rows, structType);
    }

    public static Dataset<Row> createOneFieldDf(SparkSession sqlContext, StructType structType, List<?> values) {
        final List<Row> rows = values.stream().map(DataFrameUtils::row).collect(Collectors.toList());
        return sqlContext.createDataFrame(rows, structType);
    }

    public static Row row(Object... values) {
        return RowFactory.create(values);
    }

    public static StructField structField(String columnName, DataType columnType) {
        return new StructField(columnName, columnType, true, Metadata.empty());
    }
}
