package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.time.LocalDate;

public class UpdateTriggersPartitionRemover implements IncrementSaveRemover {

    @Override
    public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean firstLoading) {
        MetastoreService metastoreService = new MetastoreService(context);
        metastoreService.dropPartition(FullTableName.of(naming.fullTableName()), "src_cd=MP");
    }
}
