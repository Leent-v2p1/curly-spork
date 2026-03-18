package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.time.LocalDate;
import java.util.List;

/**
 * AllPartitionRemover removes all partitions
 */
public class AllPartitionRemover implements IncrementSaveRemover {

    @Override
    public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean isFirstLoading) {
        if (!isFirstLoading) {
            MetastoreService metastoreService = new MetastoreService(context);
            dropPartition(context, metastoreService, naming.fullTableName());
        }
    }

    private void dropPartition(SparkSession context, MetastoreService metastoreService, String tableName) {
        final List<String> partitions = SparkSQLUtil.getPartitionList(context, tableName);
        partitions.forEach(partition -> {
            metastoreService.dropPartition(FullTableName.of(tableName), partition);
        });
    }
}
