package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryParams;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryPreset;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.generators.parser.ActionConf;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.runtime.history.RecoveredReplicaTableCreator;

import java.util.Collections;
import java.util.Optional;

import static ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.CommonFiles.PERSONALIZATION_JAR;

public class ReplicaConf extends ActionConf {
    private static final String REPLICA_RECOVERY_CLASS_NAME = RecoveredReplicaTableCreator.class.getCanonicalName();

    private final String snapshot;

    public ReplicaConf(String name,
                       String snapshot,
                       MemoryPreset memoryPreset,
                       String driverMemory,
                       String executorMemory,
                       Integer executors,
                       Integer driverMemoryOverhead,
                       Integer executorMemoryOverhead,
                       Integer sparkSqlShufflePartitions) {
        super.setName(name);
        this.setType(WorkflowType.DATAMART.getKey());
        this.snapshot = snapshot;
        MemoryParams memoryParams = memoryPreset.getParams();
        this.setDriverMemory(Optional.ofNullable(driverMemory).orElse(memoryParams.getDriverMemory()));
        this.setExecutorMemory(Optional.ofNullable(executorMemory).orElse(memoryParams.getExecutorMemory()));
        this.setExecutors(Optional.ofNullable(executors).orElse(memoryParams.getNumberOfExecutors()));
        this.setDriverMemoryOverhead(Optional.ofNullable(driverMemoryOverhead).orElse(memoryParams.getDriverMemoryOverhead()));
        this.setExecutorMemoryOverhead(Optional.ofNullable(executorMemoryOverhead).orElse(memoryParams.getExecutorMemoryOverhead()));
        this.setSparkSqlShufflePartitions(Optional.ofNullable(sparkSqlShufflePartitions).orElse(memoryParams.getSparkShufflePartitions()));
        this.setClassName(REPLICA_RECOVERY_CLASS_NAME);
        this.setJarName(PERSONALIZATION_JAR);
        this.setExtraJarNames(Collections.singletonList(PERSONALIZATION_JAR));
    }

    public ReplicaConf(String name, String snapshot, MemoryPreset memoryPreset) {
        this.setName(name);
        this.setType(WorkflowType.DATAMART.getKey());
        this.snapshot = snapshot;
        MemoryParams memoryParams = memoryPreset.getParams();
        this.setDriverMemory(memoryParams.getDriverMemory());
        this.setExecutorMemory(memoryParams.getExecutorMemory());
        this.setExecutors(memoryParams.getNumberOfExecutors());
        this.setDriverMemoryOverhead(memoryParams.getDriverMemoryOverhead());
        this.setExecutorMemoryOverhead(memoryParams.getExecutorMemoryOverhead());
        this.setSparkSqlShufflePartitions(memoryParams.getSparkShufflePartitions());
        this.setClassName(REPLICA_RECOVERY_CLASS_NAME);
        this.setJarName(PERSONALIZATION_JAR);
        this.setExtraJarNames(Collections.singletonList(PERSONALIZATION_JAR));
    }

    public ReplicaConf(String snapshot) {
        this.snapshot = snapshot;
    }

    public String getSnapshot() {
        return snapshot;
    }
}
