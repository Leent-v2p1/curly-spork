package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART_FORMAT;

public class TwoLastMonthsIncrementSaveRemover implements IncrementSaveRemover {
    @Override
    public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean firstLoading) {
        if (!firstLoading) {
            MetastoreService metastoreService = new MetastoreService(context);

            dropPartition(metastoreService, naming.fullTableName(), buildDate);
            dropPartition(metastoreService, naming.fullTableName(), buildDate.minusMonths(1));
        }
    }

    private void dropPartition(MetastoreService metastoreService, String fullTableName, LocalDate buildDate) {
        final String partitionPath = "month_part=" + buildDate.format(DateTimeFormatter.ofPattern(MONTH_PART_FORMAT));
        metastoreService.dropPartition(FullTableName.of(fullTableName), partitionPath);
    }
}
