package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.HistoricalData;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.history.KeyChecker;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history.History;

import java.time.LocalDate;

public abstract class HistoricalDatamart extends Datamart {

    protected int loadingId;
    private KeyChecker keyChecker;
    private LocalDate replicaActualityDate;

    public LocalDate replicaActualityDate() {
        return replicaActualityDate;
    }

    @Override
    public void run() {
        final History history = buildHistory();
        if (isFirstLoading) {
            firstBuild(history);
        } else {
            addDatafixes();
            dataFixApi.run(this);
            regularBuild(history);
        }
    }

    private void firstBuild(History history) {
        final Dataset<Row> firstBuiltDatamart = history.applyDefault(nonHistorical(history));
        datamartSaver.save(firstBuiltDatamart);
    }

    private void regularBuild(History history) {
        final Dataset<Row> prevDatamart = self();
        final HistoricalData datamartWithHistory = history.evalHistory(nonHistorical(history), prevDatamart);
        Dataset<Row> histFiltered=datamartWithHistory.notActual();
        histFiltered = histFiltered
                .filter("row_actual_from_dt != date_add(current_date(), -1)");

        datamartSaver.update(datamartWithHistory.actual(), histFiltered, datamartWithHistory.partitionsToAdd());
    }

    protected Dataset<Row> nonHistorical(History history) {
        final Dataset<Row> resultDatamart = buildDatamart();
        final Dataset<Row> nonHistorical = emptyStringReplacer.replaceStrings(resultDatamart);
        final Dataset<Row> savedDatamart = tempSaver.saveSelfAsTemp(nonHistorical);
        checkKeys(history, savedDatamart);
        return savedDatamart;
    }

    public void checkKeys(History history, Dataset<Row> datamart) {
        keyChecker.checkKeysNotNull(datamart, history.getIdCols(datamart));
        keyChecker.checkKeysAreUnique(datamart, history.getIdCols(datamart));
    }

    public abstract History buildHistory();

    /*Getters and setters*/

    public KeyChecker getKeyChecker() {
        return keyChecker;
    }

    public void setKeyChecker(KeyChecker keyChecker) {
        this.keyChecker = keyChecker;
    }

    public void setReplicaActualityDate(LocalDate replicaActualityDate) {
        this.replicaActualityDate = replicaActualityDate;
    }

    public int getLoadingId() {
        return loadingId;
    }

    public void setLoadingId(int loadingId) {
        this.loadingId = loadingId;
    }
}
