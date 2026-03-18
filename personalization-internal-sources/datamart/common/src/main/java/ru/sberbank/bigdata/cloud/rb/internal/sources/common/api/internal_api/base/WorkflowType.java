package ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base;

import java.util.Arrays;

/**
 * Типы сущностей в файле datamart-properties.yaml
 * В классе WorkflowGeneratorRunner используется для указания типа ResultActionConf, который далее используется для генерации shell скриптов
 */
public enum WorkflowType {

    DATAMART("datamart"),
    STAGE("stage"),
    CATALOG("catalog"),
    FORK("fork"),
    VIEW("view"),
    HELPER("helper"),
    HISTORY_STAGE("history_stage"),
    RESERVING("reserve"),
    DQCHECK("dqcheck"),
    PROPERTIES("properties"),
    REPARTITIONER("repartitioner"),
    CTL_STATS_PUBLISHER("ctl_stats_publisher"),
    SUB_WORKFLOW("sub-wf"),
    KAFKA("kafka");

    private final String key;

    WorkflowType(String type) {
        this.key = type;
    }

    public static WorkflowType valueOfByKey(String key) {
        return Arrays.stream(WorkflowType.values())
                .filter(workflowType -> workflowType.getKey().equalsIgnoreCase(key))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Cannot find key = " + key + " in enum WorkflowType"));
    }

    public String getKey() {
        return key;
    }
}
