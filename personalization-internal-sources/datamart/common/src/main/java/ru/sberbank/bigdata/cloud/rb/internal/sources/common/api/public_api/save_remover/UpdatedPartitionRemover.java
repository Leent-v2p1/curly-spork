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
 * Удаляет те партиции из витрины, которые есть в резервинговой таблице
 */
public class UpdatedPartitionRemover implements IncrementSaveRemover {

    @Override
    public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean firstLoading) {
        if (!firstLoading) {
            MetastoreService metastoreService = new MetastoreService(context);
            dropPartition(context, metastoreService, naming);
        }
    }

    private void dropPartition(SparkSession context, MetastoreService metastoreService, DatamartNaming naming) {
        List<String> partitions = SparkSQLUtil.getPartitionList(context, naming.reserveFullTableName());
        for (String partitionToDelete : partitions) {
            metastoreService.dropPartition(FullTableName.of(naming.fullTableName()), partitionToDelete);
        }
    }
}
