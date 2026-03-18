package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * Класс заменяет в переданном датафрейме строковые значения с fromToken на toToken
 */
public class StringReplacer {

    private final Logger log = LoggerFactory.getLogger(StringReplacer.class);
    private final String fromString;
    private final String toString;

    public StringReplacer(String fromToken, String toString) {
        this.fromString = fromToken;
        this.toString = toString;
    }

    public Dataset<Row> replaceStrings(Dataset<Row> inputDf) {
        log.info("Replacing strings from '{}' to '{}'", fromString, toString);
        Column[] columns = Arrays
                .stream(inputDf.schema().fields())
                .map(this::processFieldsToColumns)
                .toArray(Column[]::new);
        return inputDf.select(
                columns
        );
    }

    private Column processFieldsToColumns(StructField field) {
        final Column column = col(field.name());
        if (field.dataType() instanceof StringType) {
            return when(column.equalTo(lit(fromString)), lit(toString)).otherwise(column).as(column.toString());
        } else {
            return column;
        }
    }
}
