package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixType;

import java.util.Objects;

/**
 * Класс удаляет колонку их схемы snp и hist(если витрина историческая)
 */
public class RemoveColumn implements ChangeAction {

    private static final Logger log = LoggerFactory.getLogger(RemoveColumn.class);
    private final StructField removedColumn;

    public RemoveColumn(StructField changedColumn) {
        this.removedColumn = changedColumn;
    }

    @Override
    public StructField from() {
        return removedColumn;
    }

    @Override
    public StructField to() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoveColumn that = (RemoveColumn) o;
        return removedColumn.equals(that.removedColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(removedColumn);
    }

    @Override
    public String toString() {
        return "RemoveColumn(" + from() + ", " + to() + ")";
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> dataFrame) {
        String columnName = removedColumn.name();
        log.info("Datafix: Drop column {}", columnName);
        return dataFrame.drop(columnName);
    }

    @Override
    public DataFixType type() {
        return DataFixType.SCHEMA_CHANGE;
    }
}
