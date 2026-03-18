package ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.DatamartContext;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.sources.erib.postfixes.EribDailyByInstance;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import static org.apache.spark.sql.functions.lit;

public class EribDatamartsMergerWithInstances extends EribDatamartsMerger {

    public EribDatamartsMergerWithInstances(DatamartContext dc) {
        super(dc);
    }

    @Override
    public Dataset<Row> merge(String stgTablePrefix) {
        Dataset<Row>[] dataFrames = new Dataset[eribDailyByInstances.size()];
        for (int i = 0; i < eribDailyByInstances.size(); i++) {
            EribDailyByInstance eribDailyByInstance = eribDailyByInstances.get(i);
            String stageTableName = stgTablePrefix + eribDailyByInstance.getPostfix();
            Dataset<Row> dataFrame = dc.stageTable(stageTableName)
                    .withColumn("id_instance", lit(eribDailyByInstance.code()))
                    .withColumn("name_instance", lit(eribDailyByInstance.getPostfix()));
            dataFrames[i] = dataFrame;
        }
        return SparkSQLUtil.unionAll(dataFrames);
    }
}
