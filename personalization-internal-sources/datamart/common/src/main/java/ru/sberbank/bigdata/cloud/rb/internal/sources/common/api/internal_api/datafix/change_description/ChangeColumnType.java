package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixType;

import java.util.*;

import static java.util.Collections.emptyList;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Класс изменяет тип колонки в схеме snp и hist(если витрина историческая)
 */
public class ChangeColumnType implements ChangeAction {

    private static final Logger log = LoggerFactory.getLogger(ChangeColumnType.class);
    private static final Map<DataType, List<DataType>> CONVERSION_TABLE = new HashMap<>();
    private final StructField sourceColumn;
    private final StructField targetColumn;

    public ChangeColumnType(StructField sourceColumn, StructField targetColumn) {
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    /**
     * @return Map, в которой
     * ключ - тип, из которого конвертируем
     * значение - список типов, в который тип ключа конвертируется без дополнительных проверок и условий
     */
    private static Map<DataType, List<DataType>> getConversionMap() {
        add(ByteType,      /*:*/ShortType, IntegerType, LongType, FloatType, DoubleType, StringType);
        add(ShortType,     /*:*/IntegerType, LongType, FloatType, DoubleType, StringType);
        add(IntegerType,   /*:*/LongType, FloatType, DoubleType, StringType);
        add(LongType,      /*:*/FloatType, DoubleType, StringType);
        add(FloatType,     /*:*/DoubleType, StringType);
        add(DoubleType,    /*:*/StringType);
        add(TimestampType, /*:*/StringType);
        add(DateType,      /*:*/StringType);
        return CONVERSION_TABLE;
    }

    static boolean isCastPossible(DataType fromType, DataType toType) {
        if (toType instanceof DecimalType) {
            return ((DecimalType) toType).isWiderThan(fromType);
        } else if (fromType instanceof DecimalType) {
            return ((DecimalType) fromType).isTighterThan(toType) || toType instanceof StringType;
        } else {
            return getConversionMap().getOrDefault(fromType, emptyList()).contains(toType);
        }
    }

    private static void add(DataType from, DataType... to) {
        CONVERSION_TABLE.computeIfAbsent(from, k -> new ArrayList<>()).addAll(Arrays.asList(to));
    }

    @Override
    public StructField from() {
        return sourceColumn;
    }

    @Override
    public StructField to() {
        return targetColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChangeColumnType that = (ChangeColumnType) o;
        return sourceColumn.equals(that.sourceColumn) &&
                targetColumn.equals(that.targetColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceColumn, targetColumn);
    }

    @Override
    public String toString() {
        return "ChangeColumnType(" + from() + ", " + to() + ")";
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> dataFrame) {
        final String columnName = sourceColumn.name();

        final DataType fromType = sourceColumn.dataType();
        final DataType toType = targetColumn.dataType();

        if (isCastPossible(fromType, toType)) {
            log.info("Datafix: Change datatype of column '{}' from {} to {}", columnName, fromType, toType);
            return dataFrame
                    .withColumn(columnName, dataFrame.col(columnName).cast(toType));
        } else if (fromType == StringType && Arrays.asList(ByteType, ShortType, IntegerType, LongType).contains(toType)) {
            log.info("Datafix: Change datatype of column '{}' from {} to {}", columnName, fromType, toType);
            return dataFrame
                    .withColumn(columnName, coalesce(dataFrame.col(columnName).cast(toType), lit("-1").cast(toType)));
        } else {
            throw new IllegalStateException("Cast from " + fromType + " to " + toType + " impossible");
        }
    }

    @Override
    public DataFixType type() {
        return DataFixType.SCHEMA_CHANGE;
    }
}
