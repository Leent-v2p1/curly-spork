package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.DatamartProperties;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ParentDatamartResolver {

    private Map<String, Object> customProperties;

    public void setCustomProperties(Map<String, Object> customProperties) {
        this.customProperties = customProperties;
    }

    public FullTableName getDatamartName(FullTableName datamartId, WorkflowType type) {
        while (type == WorkflowType.STAGE) {
            DatamartProperties property = parentDatamart(datamartId.fullTableName());
            datamartId = property.getDatamartId();
            type = property.getWorkflowType();
        }
        return datamartId;
    }

    private DatamartProperties parentDatamart(String branchId) {
        List<DatamartProperties> matches = PropertiesUtils.getAllActionsId(customProperties).stream()
                .map(FullTableName::of)
                .map(tableName -> new DatamartProperties(tableName, customProperties))
                .filter(parent -> hasDependency(branchId, parent))
                .collect(Collectors.toList());

        if (matches.size() != 1) {
            throw new IllegalArgumentException("Expected 1 element, but found " + matches.size());
        }
        return matches.get(0);
    }

    private boolean hasDependency(String branchId, DatamartProperties parent) {
        return parent.getDependencies()
                .stream()
                .flatMap(this::resolveFork)
                .anyMatch(currId -> currId.equals(branchId));
    }

    @SuppressWarnings("unchecked")
    private Stream<String> resolveFork(Object dependency) {
        if (dependency instanceof String) {
            return Stream.of((String) dependency);
        }
        if (dependency instanceof Map) {
            return getBranches((Map<String, Map<String, Object>>) dependency).stream()
                    .flatMap(this::resolveFork);
        }
        throw new IllegalArgumentException("Wrong dependency type: " + dependency.getClass());
    }

    @SuppressWarnings("unchecked")
    private List<Object> getBranches(Map<String, Map<String, Object>> dependency) {
        final Map<String, Object> fork = dependency.get("fork");
        return (List<Object>) fork.get("branches");
    }
}
