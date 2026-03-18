package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HistoricalDatamartsExtractor {
    private Map<String, Object> customProperties;

    void setCustomProperties(Map<String, Object> customProperties) {
        this.customProperties = customProperties;
    }

    public Map<FullTableName, DatamartProperties> getHistoricalDatamarts() {
        return PropertiesUtils
                .getAllActionsIdWithType(WorkflowType.DATAMART, customProperties)
                .stream()
                .map(FullTableName::of)
                .map(tableName -> new DatamartProperties(tableName, customProperties))
                .filter(propertiesService -> !propertiesService.getSourceReplicaTables().isEmpty())
                .collect(Collectors.toMap(
                        config -> FullTableName.of(config.getTargetSchema(), config.getTargetTable()),
                        Function.identity())
                );
    }
}