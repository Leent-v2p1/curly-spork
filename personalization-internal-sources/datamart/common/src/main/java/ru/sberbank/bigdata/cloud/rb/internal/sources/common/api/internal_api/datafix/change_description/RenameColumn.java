package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixType;

import java.util.Objects;

/**
 * Класс переименовывает колонку в схемах snp и hist(если витрина историческая)
 */
public class RenameColumn implements ChangeAction {

    private static final Logger log = LoggerFactory.getLogger(RenameColumn.class);
    private final StructField fromColumn;
    private final StructField toColumn;

    public RenameColumn(StructField fromColumn, StructField toColumn) {
        this.fromColumn = fromColumn;
        this.toColumn = toColumn;
    }

    @Override
    public StructField from() {
        return fromColumn;
    }

    @Override
    public StructField to() {
        return toColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RenameColumn that = (RenameColumn) o;
        return fromColumn.equals(that.fromColumn) &&
                toColumn.equals(that.toColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromColumn, toColumn);
    }

    @Override
    public String toString() {
        return "RenameColumn(" + from() + ", " + to() + ")";
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> dataFrame) {
        final String from = fromColumn.name();
        final String to = toColumn.name();
        log.info("Datafix: Renaming column from {} to {}", from, to);
        return dataFrame.withColumnRenamed(from, to);
    }

    @Override
    public DataFixType type() {
        return DataFixType.SCHEMA_CHANGE;
    }
}
