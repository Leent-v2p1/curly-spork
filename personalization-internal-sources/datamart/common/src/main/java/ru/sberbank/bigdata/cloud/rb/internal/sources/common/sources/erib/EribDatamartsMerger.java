package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes.EribDailyByInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class EribDatamartsMerger {
    protected final DatamartContext dc;
    protected final List<EribDailyByInstance> eribDailyByInstances;

    public EribDatamartsMerger(DatamartContext dc) {
        this.dc = dc;
        this.eribDailyByInstances = asList(EribDailyByInstance.values());
    }

    public Dataset<Row> merge(String stgTablePrefix) {
        requireNonNull(dc);
        Dataset<Row>[] stgTables =
                eribDailyByInstances.stream()
                        .map(eribSourceSchemaPostfix -> stgTablePrefix + eribSourceSchemaPostfix.getPostfix())
                        .map(dc::targetTable)
                        .toArray(Dataset[]::new);
        return SparkSQLUtil.unionAll(stgTables);
    }
}
