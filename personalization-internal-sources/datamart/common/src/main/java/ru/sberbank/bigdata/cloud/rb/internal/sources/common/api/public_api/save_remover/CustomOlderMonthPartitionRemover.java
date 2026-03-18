package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_remover;

import org.apache.spark.sql.SparkSession;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.hive.MetastoreService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.DatamartNaming;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.save_remover.IncrementSaveRemover;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.util.sql.SparkSQLUtil;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.FieldConstants.*;


abstract public class CustomOlderMonthPartitionRemover implements IncrementSaveRemover {

    @Override
    abstract public void remove(SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean isFirstLoading);

    private void dropPartitionName(String namePartition, SparkSession context, MetastoreService metastoreService, String tableName,
                               LocalDate lowerBound) {
        final List<String> partitions = SparkSQLUtil.getPartitionList(context, tableName);
        partitions.stream()
                .filter(partition -> {
                    final String[] fullPart = partition.split("/");
                    final String targetPart = Arrays.stream(fullPart).filter(el -> el.split("=")[0].equals(namePartition))
                            .findAny()
                            .get();
                    final String[] partitionInfo = targetPart.split("=");
                    final LocalDate partitionDate = getPartitionDatePattern(partitionInfo);
                    return partitionDate.isBefore(lowerBound);
                })
                .forEach(partition -> {
                    metastoreService.dropPartition(FullTableName.of(tableName), partition);
                });
    }

    private LocalDate getPartitionDatePattern(String[] partitionInfo) {
        final String partitionDate;
        String patternDayPart = "\\d{4}-\\d{2}-\\d{2}";
        String patternMonthPart = "\\d{4}-\\d{2}";
        if (Pattern.matches(patternDayPart, partitionInfo[1])) {
            partitionDate = partitionInfo[1];
        } else if (Pattern.matches(patternMonthPart, partitionInfo[1])) {
            partitionDate = partitionInfo[1] + "-01";
        } else {
            throw new IllegalArgumentException("Unsupported type of partition: " + partitionInfo[0]);
        }
        return LocalDate.parse(partitionDate, DateTimeFormatter.ofPattern(DAY_PART_FORMAT));
    }
    protected void remove(String namePart, SparkSession context, DatamartNaming naming, LocalDate buildDate, boolean isFirstLoading, Integer numMonth) {
        if (!isFirstLoading) {
            MetastoreService metastoreService = new MetastoreService(context);
            dropPartitionName(namePart, context, metastoreService, naming.fullTableName(), buildDate.minusMonths(numMonth).withDayOfMonth(1));
        }
    }
}
