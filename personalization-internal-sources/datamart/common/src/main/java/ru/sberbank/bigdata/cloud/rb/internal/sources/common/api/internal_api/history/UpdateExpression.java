package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Created by sbt-parkhomenko-os on 12.05.2017.
 */
public interface UpdateExpression {

    /**
     * @param field name of column to watch updates for
     */
    static UpdateExpression fieldUpd(String field) {
        return (prev, curr) -> {
            final Column prevCol = coalesce(prev.col(field).cast(StringType), lit("NULL"));
            final Column currCol = coalesce(curr.col(field).cast(StringType), lit("NULL"));
            return prevCol.notEqual(currCol);
        };
    }

    /**
     * @param fields names of columns to watch updates for
     */
    static UpdateExpression fieldsUpd(String... fields) {
        return (prev, curr) -> {
            final Column concatPrev = concat(Arrays.stream(fields)
                    .map(prev::col)
                    .map(c -> coalesce(c.cast(StringType), lit("NULL")))
                    .toArray(Column[]::new));
            final Column concatCurr = concat(Arrays.stream(fields)
                    .map(curr::col)
                    .map(c -> coalesce(c.cast(StringType), lit("NULL")))
                    .toArray(Column[]::new));
            return concatCurr.notEqual(concatPrev);
        };
    }

    /**
     * Метод используется для проверки, что в строке изменилась хотя бы одна колонка
     * берем набор колонок из curr, потому что в них нет служебных полей.
     */
    static UpdateExpression rowUpdated() {
        return (prev, curr) -> UpdateExpression.fieldsUpd(curr.columns()).expr(prev, curr);
    }

    static UpdateExpression rowUpdatedViaSha2() {
        return (prev, curr) -> {
            final Column rowHash = hashViaSha2(curr);
            return prev.col("row_hash").notEqual(rowHash);
        };
    }

    static Column hashViaSha2(Dataset<Row> df) {
        final Column concat = concat(Arrays.stream(df.columns())
                .map(df::col)
                .map(c -> coalesce(c.cast(StringType), lit("NULL")))
                .toArray(Column[]::new));
        return functions.sha2(concat, 256);
    }

    Column expr(Dataset<Row> prev, Dataset<Row> curr);
}
