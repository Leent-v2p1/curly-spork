package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix;

import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.datafix.change_description.ChangeAction;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.auto_config.DatamartServiceFactory;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.base.Datamart;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.history.History;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.hive.PartitionInfo;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.HiveSavingStrategy;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.save_strategy.PartitionedSavingStrategy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnValidator {

    private static final Logger log = LoggerFactory.getLogger(ColumnValidator.class);

    private final DatamartServiceFactory serviceFactory;

    public ColumnValidator(DatamartServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
    }

    /**
     * Validate that schema change is not changing table id  columns or partition columns
     */
    public void checkSchemaChanges(Datamart datamart,
                                   List<ChangeAction> changes) {
        final Set<String> changedColumns = changes.stream()
                .map(changeAction -> Optional.ofNullable(changeAction.from()).orElse(changeAction.to()))
                .map(StructField::name)
                .collect(Collectors.toSet());

        validateHistoricalDatamartId(changedColumns);
        validatePartitionColumn(changedColumns, datamart);
    }

    private void validateHistoricalDatamartId(Set<String> changedColumns) {
        final Optional<History> history = serviceFactory.parametersService().getHistory(serviceFactory.datamartContext());
        if (history.isPresent()) {
            log.info("Datamart is historical, checking id columns");
            final List<String> idColumnNames = history.get().getIds();
            validateNoIntersection(changedColumns, idColumnNames, "There's intersection between ID and CHANGED columns: %s");
            log.info("Check is ok, no changes to id columns");
            return;
        }
        log.info("Datamart is not historical");
    }

    private void validatePartitionColumn(Set<String> changedColumns,
                                         Datamart datamart) {
        final HiveSavingStrategy hiveSavingStrategy = datamart.getDatamartSaver().getSavingStrategy();
        if (hiveSavingStrategy instanceof PartitionedSavingStrategy) {
            log.info("Datamart has custom PartitionedSavingStrategy, checking partition columns");
            PartitionedSavingStrategy partitionedSavingStrategy = (PartitionedSavingStrategy) hiveSavingStrategy;
            final PartitionInfo partitionInfo = partitionedSavingStrategy.getPartitionInfo();
            final List<String> partitionColumns = partitionInfo.colNames();
            validateNoIntersection(changedColumns, partitionColumns, "There's intersection between PARTITION_COLUMNS and CHANGED columns: %s");
            log.info("Check is ok, no changes to partition columns");
            return;
        }
        log.info("Datamart doesn't have custom PartitionedSavingStrategy");
    }

    private void validateNoIntersection(Set<String> changedColumns, List<String> idColumnNames, String exceptionTemplate) {
        final List<String> intersect = changedColumns
                .stream()
                .filter(idColumnNames::contains)
                .collect(Collectors.toList());
        if (!intersect.isEmpty()) {
            final String exceptionMessage = String.format(exceptionTemplate, intersect.toString());
            throw new IllegalStateException(exceptionMessage);
        }
    }
}
