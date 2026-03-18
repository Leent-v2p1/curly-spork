package ru.sberbank.bigdata.cloud.rb.internal.sources.generator.impl.replica_on_date.configuration;

import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.base.WorkflowType;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.api.internal_api.properties.PropertiesUtils;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.constants.NameAdditions;
import ru.sberbank.bigdata.cloud.rb.internal.sources.common.service.reserving.ReservingService;
import ru.sberbank.bigdata.cloud.rb.internal.sources.generator.api.oozie.entities.MemoryPreset;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReplicaActionDependency {
    private final Map<String, Object> replicaProperties;

    public ReplicaActionDependency(Map<String, Object> replicaProperties) {
        this.replicaProperties = replicaProperties;
    }

    public Map<String, ReplicaConf> resolve() {
        return PropertiesUtils
                .getAllActionsIdWithType(WorkflowType.HISTORY_STAGE, replicaProperties)
                .stream()
                .flatMap(name -> {
                    ReplicaConf replicaConf = createReplicaConf(name);
                    ReplicaConf propertiesConf = createPropertiesConf(replicaConf);
                    replicaConf.setDependencies(Collections.singletonList(propertiesConf.getName()));
                    return Stream.of(replicaConf, propertiesConf);
                })
                .collect(Collectors.toMap(ReplicaConf::getName, Function.identity()));
    }

    private ReplicaConf createReplicaConf(String name) {
        String snapshot = (String) replicaProperties.get(name + ".replicaSnapshotTable");
        String memoryPreset = (String) replicaProperties.get(name + ".memoryPreset");
        MemoryPreset preset = memoryPreset != null ? MemoryPreset.valueOf(memoryPreset) : MemoryPreset.NOT_SPECIFIED;
        Integer executors = (Integer) replicaProperties.get(name + ".executors");
        String executorMemory = (String) replicaProperties.get(name + ".executorMemory");
        String driverMemory = (String) replicaProperties.get(name + ".driverMemory");
        Integer driverOverhead = (Integer) replicaProperties.get(name + ".driverMemoryOverhead");
        Integer executorOverhead = (Integer) replicaProperties.get(name + ".executorMemoryOverhead");
        Integer shufflePartitions = (Integer) replicaProperties.get(name + ".sparkSqlShufflePartitions");
        return new ReplicaConf(name, snapshot, preset, driverMemory, executorMemory, executors, driverOverhead, executorOverhead, shufflePartitions);
    }

    private ReplicaConf createPropertiesConf(ReplicaConf datamartConf) {
        ReplicaConf resultedConf = new ReplicaConf(datamartConf.getSnapshot());
        resultedConf.setName(datamartConf.getName() + NameAdditions.PROPERTIES_POSTFIX);
        resultedConf.setType(WorkflowType.PROPERTIES.getKey());
        resultedConf.setStart(false);
        resultedConf.setClassName(ReservingService.class.getName());
        return resultedConf;
    }
}
