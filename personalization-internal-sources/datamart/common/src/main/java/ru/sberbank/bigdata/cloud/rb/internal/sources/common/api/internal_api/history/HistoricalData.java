package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Set;
import java.util.function.Supplier;

public class HistoricalData {

    private final Supplier<Dataset<Row>> actual;
    private final Supplier<Dataset<Row>> notActual;
    private final Supplier<Set<String>> partitionsToAdd;

    public HistoricalData(Supplier<Dataset<Row>> actual, Supplier<Dataset<Row>> notActual, Supplier<Set<String>> partitionsToAdd) {
        this.actual = actual;
        this.notActual = notActual;
        this.partitionsToAdd = partitionsToAdd;
    }

    public Dataset<Row> actual() {
        return actual.get();
    }

    public Dataset<Row> notActual() {
        return notActual.get();
    }

    public Set<String> partitionsToAdd() {
        return partitionsToAdd.get();
    }
}
