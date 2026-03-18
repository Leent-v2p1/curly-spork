package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.MONTH_PART_FORMAT;

public class DepAgrmntAggrIncrementSaveRemover implements IncrementSaveRemover {

    @Override
    public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean isFirstLoading) {
        if (isFirstLoading) {
            // needs keep dep_agrmnt_state only for one year, but for this datamart init loading we need dep_agrmnt_state from 2012 year,
            // so we can remove unused partitions after init loading
            MetastoreService metastoreService = new MetastoreService(context);
            final String depAgrmntStateTableName = naming.targetWithPostfix("dep_agrmnt_state");
            removeDepAgrmntStatePartitions(context, depAgrmntStateTableName, metastoreService, buildDate.minusYears(1));
        }
    }

    private void removeDepAgrmntStatePartitions(SparkSession context,
                                                String depAgrmntStateTableName,
                                                MetastoreService metastoreService,
                                                LocalDate lowerBound) {
        final List<String> partitions = SparkSQLUtil.getPartitionList(context, depAgrmntStateTableName);
        partitions.stream()
                .map(partitionPairs -> {
                    final String[] yyyyMM = partitionPairs
                            .split("=")[1]
                            .split("-");
                    final String year = yyyyMM[0];
                    final String month = yyyyMM[1];
                    return LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), 1);
                })
                .filter(partitionDate -> partitionDate.isBefore(lowerBound))
                .forEach(partitionDate -> {
                    final String partitionPath = "month_part=" + partitionDate.format(DateTimeFormatter.ofPattern(MONTH_PART_FORMAT));
                    metastoreService.dropPartition(FullTableName.of(depAgrmntStateTableName), partitionPath);
                });
    }
}
