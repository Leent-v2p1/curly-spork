package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.DataFixType;

import java.util.Objects;

import static org.apache.spark.sql.functions.lit;

/**
 * Класс добавляет колонку в схему snp и hist(если витрина историческая)
 */
public class AddColumn implements ChangeAction {

    private static final Logger log = LoggerFactory.getLogger(AddColumn.class);
    private final StructField addedColumn;

    public AddColumn(StructField addedColumn) {
        this.addedColumn = addedColumn;
    }

    @Override
    public StructField from() {
        return null;
    }

    @Override
    public StructField to() {
        return addedColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AddColumn addColumn = (AddColumn) o;
        return addedColumn.equals(addColumn.addedColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(addedColumn);
    }

    @Override
    public String toString() {
        return "AddColumn(" + from() + ", " + to() + ")";
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> dataFrame) {
        String columnName = addedColumn.name();
        log.info("Datafix: Add empty column {}", columnName);
        return dataFrame.withColumn(columnName, lit(null).cast(addedColumn.dataType()));
    }

    @Override
    public DataFixType type() {
        return DataFixType.SCHEMA_CHANGE;
    }
}
