package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.naming.ParentDatamartResolver;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.FullTableName;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.public_api.naming.SchemaAlias;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toMap;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonDirs.DATAMART_PREFIX_PATH;
import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonFiles.PERSONALIZATION_JAR;

/**
 * This class is used to store parameters from datamart-properties, which are used in generating properties for runtime
 */
public class DatamartProperties extends BaseProperties {
    private final FullTableName datamartId;

    public DatamartProperties(FullTableName datamartId) {
        this(datamartId, null);
    }

    public DatamartProperties(FullTableName datamartId, Map<String, Object> customProperties) {
        super(customProperties);
        this.datamartId = datamartId;
        if (properties().keySet().stream().noneMatch((s -> s.startsWith(datamartId.fullTableName())))) {
            throw new IllegalStateException(String.format("Datamart with datamartId: \"%s\" doesn't exist", datamartId));
        }
    }

    public FullTableName getDatamartId() {
        return datamartId;
    }

    public Optional<Integer> getCtlEntityId() {
        Integer entityId = (Integer) properties().get(parentDatamartName() + ".ctl.entity.id");
        return Optional.ofNullable(entityId);
    }

    public String getJarName() {
        if (getClassName().startsWith(DATAMART_PREFIX_PATH)) {
            final String schemaAlias = SchemaAlias.of(getTargetSchema()).schemaAliasValue;
            return schemaAlias + "-datamart.jar";
        } else {
            return PERSONALIZATION_JAR;
        }
    }

    public FullTableName parentDatamartName() {
        ParentDatamartResolver resolver = new ParentDatamartResolver();
        if (customProperties != null) {
            resolver.setCustomProperties(customProperties);
        }

        return resolver.getDatamartName(datamartId, getWorkflowType());
    }

    public String getViewSourceTable() {
        return (String) properties().get(datamartId + ".sourceTable");
    }

    public String getViewSqlTemplate() {
        return (String) properties().get(datamartId + ".sqlTemplate");
    }

    public String getTargetSchema() {
        return datamartId.dbName();
    }

    public String getTargetTable() {
        return datamartId.tableName();
    }

    public String getSourceSchema() {
        return (String) properties().get(datamartId + ".sourceSchema");
    }

    public WorkflowType getWorkflowType() {
        String workflowTypeProperty = (String) properties().get(datamartId + ".type");
        return workflowTypeProperty != null
                ? WorkflowType.valueOf(workflowTypeProperty.toUpperCase())
                : null;
    }

    public String getClassName() {
        return (String) properties().get(datamartId + ".class");
    }

    public Integer getMonthlyDependencyDay() {
        return (Integer) properties().get(datamartId + ".monthlyDependencyDay");
    }

    public String getMemoryPreset() {
        return (String) properties().get(datamartId + ".memoryPreset");
    }

    public Integer getExecutors() {
        return (Integer) properties().get(datamartId + ".executors");
    }

    public String getExecutorMemory() {
        return (String) properties().get(datamartId + ".executorMemory");
    }

    public Integer getExecutorCoreNum() {
        return (Integer) properties().get(datamartId + ".executorCoreNum");
    }

    public String getDriverMemory() {
        return (String) properties().get(datamartId + ".driverMemory");
    }

    public Integer getDriverMemoryOverhead() {
        return (Integer) properties().get(datamartId + ".driverMemoryOverhead");
    }

    public Integer getExecutorMemoryOverhead() {
        return (Integer) properties().get(datamartId + ".executorMemoryOverhead");
    }

    public Integer getSparkSqlShufflePartitions() {
        return (Integer) properties().get(datamartId + ".sparkSqlShufflePartitions");
    }

    public Map<String, String> customJavaOpts() {
        return getPropertiesMap(datamartId + ".customJavaOpts");
    }

    private Map<String, String> getPropertiesMap(String prefix) {
        return properties().entrySet().stream()
                .filter(keyValue -> keyValue.getKey().startsWith(prefix))
                .collect(toMap(keyValue -> keyValue.getKey().replaceFirst(prefix + ".", ""),
                        entry -> entry.getValue().toString()));
    }

    public ReservingProperties reservingProperties() {
        String datamartReservingId = datamartId + ".reserving";
        String driverMemoryForReserve = (String) properties().get(datamartReservingId + ".driverMemory");
        Map<String, String> customJavaOpts = getPropertiesMap(datamartReservingId + ".customJavaOpts");
        return new ReservingProperties(datamartId, customProperties, driverMemoryForReserve, customJavaOpts);
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getSourceReplicaTables() {
        return (List<T>) properties().getOrDefault(datamartId + ".sourceReplicaTables", Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getDependencies() {
        return (List<T>) properties().getOrDefault(datamartId + ".dependencies", Collections.emptyList());
    }
}