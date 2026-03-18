package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.UpdateExpression;

import java.util.function.Function;

import static org.apache.spark.sql.functions.col;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.evaluator.HistoryEvaluator.IS_UPDATED_COLUMN;

public class HistColumn {

    private final String name;
    private final UpdateExpression upd;
    private final UpdateExpression initValue;
    private final UpdateExpression updatedValue;
    private final UpdateExpression staleValue;
    private final UpdateExpression deletedValue;
    private final boolean needsAdditionalTables;

    private HistColumn(String name,
                       UpdateExpression upd,
                       UpdateExpression initValue,
                       UpdateExpression updatedValue,
                       UpdateExpression staleValue,
                       UpdateExpression deletedValue,
                       boolean needsAdditionalTables) {
        this.name = name;
        this.upd = upd;
        this.initValue = initValue;
        this.updatedValue = updatedValue;
        this.staleValue = staleValue;
        this.deletedValue = deletedValue;
        this.needsAdditionalTables = needsAdditionalTables;
    }

    public static Builder histCol(String name) {
        return new Builder(name);
    }

    public String name() {
        return name;
    }

    public Column isUpdated(Dataset<Row> oldDatamart, Dataset<Row> newDatamart) {
        return upd.expr(oldDatamart, newDatamart);
    }

    public Column initValue(Dataset<Row> newDatamart) {
        return initValue.expr(newDatamart, newDatamart);
    }

    public Column updatedValue(Dataset<Row> oldDatamart, Dataset<Row> newDatamart) {
        return updatedValue.expr(oldDatamart, newDatamart);
    }

    public Column staleValue(Dataset<Row> oldDatamart, Dataset<Row> newDatamart) {
        return staleValue.expr(oldDatamart, newDatamart);
    }

    public Column deletedValue(Dataset<Row> oldDatamart, Dataset<Row> newDatamart) {
        return deletedValue.expr(oldDatamart, newDatamart);
    }

    public boolean isNeedsAdditionalTables() {
        return needsAdditionalTables;
    }

    public static class Builder {

        private final String name;
        private UpdateExpression isUpdated;
        private UpdateExpression init;
        private boolean needsAdditionalTables = true;
        private UpdateExpression updVal;
        private UpdateExpression staleVal;
        private UpdateExpression deletedVal;

        public Builder(String name) {
            this.name = name;
        }

        public Builder when(UpdateExpression isUpdated) {
            this.isUpdated = isUpdated;
            return this;
        }

        public Builder whenFieldChanges(String field) {
            this.isUpdated = UpdateExpression.fieldUpd(field);
            return this;
        }

        public Builder eachTime() {
            this.isUpdated = (prev, curr) -> functions.lit(true);
            return this;
        }

        public Builder whenRowCreated() {
            this.isUpdated = (prev, curr) -> prev.col(name).isNull();
            return this;
        }

        public Builder whenRowUpdated() {
            this.isUpdated = ((prev, curr) -> col(IS_UPDATED_COLUMN));
            return this;
        }

        public Builder initValue(Function<Dataset<Row>, Column> init) {
            this.init = (prev, curr) -> init.apply(curr);
            return this;
        }

        public Builder initValue(Function<Dataset<Row>, Column> init, boolean needsAdditionalTables) {
            this.init = (prev, curr) -> init.apply(curr);
            this.needsAdditionalTables = needsAdditionalTables;
            return this;
        }

        public Builder initValue(Column column) {
            this.init = (prev, curr) -> column;
            return this;
        }

        public Builder updatedValue(UpdateExpression updVal) {
            this.updVal = updVal;
            return this;
        }

        public Builder updatedValue(Column column) {
            this.updVal = (prev, curr) -> column;
            return this;
        }

        public Builder staleValue(UpdateExpression staleVal) {
            this.staleVal = staleVal;
            return this;
        }

        public Builder staleValue(Column column) {
            this.staleVal = (prev, curr) -> column;
            return this;
        }

        public Builder deletedValue(Column column) {
            this.deletedVal = (prev, curr) -> column;
            return this;
        }

        public Builder deletedValue(UpdateExpression deletedVal) {
            this.deletedVal = deletedVal;
            return this;
        }

        public HistColumn create() {
            if (init == null && updVal == null) {
                throw new IllegalStateException("Either initial or updated column value must be specified for " + name);
            }
            if (staleVal == null) {
                staleVal = (prev, curr) -> prev.col(name);
            }
            if (init == null) {
                init = updVal;
            } else if (updVal == null) {
                updVal = init;
            }

            if (deletedVal == null) {
                deletedVal = staleVal;
            }
            return new HistColumn(name, isUpdated, init, updVal, staleVal, deletedVal, needsAdditionalTables);
        }
    }
}
